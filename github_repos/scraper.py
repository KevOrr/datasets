import json
import time
import calendar
import sys
import logging as _logging

import requests
from sqlalchemy import exists
from sqlalchemy.orm.exc import NoResultFound

from github_repos.db import Session
from github_repos.db import Repo, NewRepo, ReposTodo, RepoError
from github_repos.db import Owner, OwnerType
from github_repos.db import RepoLanguages, Language, QueryCost
import github_repos.config as g


log = _logging.getLogger(__name__)

MAX_EXPAND_ERRORS = 2
MAX_FETCH_ERRORS = 30
class MaxErrors(RuntimeError):
    pass

class GithubTimeout(RuntimeError):
    def __init__(self, errors, todo):
        RuntimeError.__init__(self, repr(errors))
        self.todo = todo
        self.errors = errors

class EmptyResultError(RuntimeError):
    def __init__(self, errors, todo):
        RuntimeError.__init__(self, repr(errors))
        self.todo = todo
        self.errors = errors

class GraphQLException(RuntimeError):
    pass

class RateLimit(RuntimeError):
    pass


POPULAR_REPOS_QUERY = '''\
query {
  search(type: REPOSITORY, query: "stars:>1 sort:stars", first: 100) {
    nodes {
      ...on Repository {
        name
        stargazers() {
          totalCount
        }
        owner {
          __typename
          login
        }
      }
    }
  }
}'''

EXPAND_COST_GUESS = 1300
EXPAND_QUERY = '''\
query($owner:String!, $name:String!) {
  repository(owner: $owner, name: $name) {
    mentionableUsers(first: 5) {
      nodes {
        ...userExpand
      }
    }
    stargazers(first: 5, orderBy: {field: STARRED_AT, direction: DESC}) {
      nodes {
        ...userExpand
      }
    }
    watchers(first: 5) {
      nodes {
        ...userExpand
      }
    }
  }
  rateLimit {
    cost
    remaining
    resetAt
  }
}

fragment userExpand on User {
  contributedRepositories(first: 10, privacy: PUBLIC, orderBy: {field: STARGAZERS, direction: DESC}) {
    nodes {
      ...repoInfo
    }
  }
  issues(first: 20, orderBy: {field: COMMENTS, direction: DESC}) {
    nodes {
      repository {
        ...repoInfo
      }
    }
  }
  pullRequests(first: 20) {
    nodes {
      repository {
        ...repoInfo
      }
    }
  }
  starredRepositories(first: 20, orderBy: {field: STARRED_AT, direction: DESC}) {
    nodes {
      ...repoInfo
    }
  }
}

fragment repoInfo on Repository {
  name
  owner {
    __typename
    login
  }
}
'''

fetch_cost_guess = lambda n: n * 1.5 / 467.0 # Best seen so far
FETCH_QUERY = '''\
query {
%s
  rateLimit {
    cost
    remaining
    resetAt
  }
}

fragment repoInfo on Repository {
  name
  owner {
    login
  }
  description
  diskUsage
  url
  isFork
  isMirror
  languages(first: 10, orderBy: {field: SIZE, direction: DESC}) {
    edges {
      size
      node {
        name
        color
      }
    }
  }
}'''

RATE_LIMIT_QUERY = '''\
query {
  rateLimit {
    resetAt
  }
}'''


def strp_reset_time(time_str):
    return time.strptime(time_str, "%Y-%m-%dT%H:%M:%Sz")

def send_query(query, variables=None, url=g.api_url, raw=False, api_key=g.personal_token, sender=requests.post):
    if not isinstance(query, str):
        query = query.format()

    headers = {'Authorization': 'bearer ' + api_key}
    if variables:
        result = sender(url, headers=headers, data=json.dumps({'query': query, 'variables': variables}))
    else:
        result = sender(url, headers=headers, data=json.dumps({'query': query}))

    if raw:
        result = result.text
    else:
        result = result.json()

    return result

def get_repos_from_user_nodes(user_nodes):
    repos = set()
    count = 0

    for key in ('contributedRepositories', 'starredRepositories', 'issues', 'pullRequests'):
        for user in user_nodes:
            for node in user.get(key, {}).get('nodes', []):
                try:
                    repo = node if key in ('contributedRepositories', 'starredRepositories') else node['repository']
                    repos.add(((repo['owner']['login'], repo['owner']['__typename']), repo['name']))
                    count += 1
                except KeyError:
                    pass

    return repos, count


class MainScraper():
    def __init__(self):
        self.session = Session()
        self.rate_limit_remaining = 5000
        self.expand_errors = {}
        self.fetch_errors = 0
        self.reset_time = time.time() + 3600


    def owner_exists(self, login):
        return self.session.query(exists().where(Owner.login == login)).scalar()

    def repo_fetched(self, owner_login, repo_name):
        return self.session.query(
            self.session.query(Owner).filter_by(login=owner_login).join(Repo).filter_by(name=repo_name).exists()
        ).scalar()

    def repo_found_not_fetched(self, owner_login, repo_name):
        return self.session.query(
            self.session.query(Owner).filter_by(login=owner_login).join(NewRepo).filter(NewRepo.name == repo_name).exists()
        ).scalar()

    def repo_exists(self, owner_login, repo_name):
        return self.repo_fetched(owner_login, repo_name) or self.repo_found_not_fetched(owner_login, repo_name)


    def rate_limit_sleep(self):
        result = send_query(RATE_LIMIT_QUERY)
        if 'data' in result and 'rateLimit' in result['data'] and 'resetAt' in result['data']['rateLimit']:
            self.reset_time = calendar.timegm(strp_reset_time(result['data']['rateLimit']['resetAt']))

        now = time.time()
        sleep_time = 30 + self.reset_time - now
        log.info('Sleeping %d seconds until %s', sleep_time, time.asctime(time.localtime(now + sleep_time)))
        time.sleep(sleep_time)
        self.rate_limit_remaining = 5000


    def populate_most_popular(self):
        '''Gets 100 most starred repos from github search'''

        result = send_query(POPULAR_REPOS_QUERY)

        with self.session.begin_nested():
            for repo in sorted(result.get('data', {}).get('search', {}).get('nodes', []),
                               key=lambda repo:repo['stargazers']['totalCount'],
                               reverse=True):
                try:
                    owner = self.session.query(Owner).filter_by(login=repo['owner']['login']).one()
                except NoResultFound:
                    owner = Owner(login=repo['owner']['login'],
                                  owner_type=self.session.query(OwnerType).filter_by(typename=repo['owner']['__typename']).one())
                    self.session.add(owner)

                new_repo = NewRepo(name=repo['name'], owner=owner)
                self.session.add(new_repo)

        self.session.commit()

    def expand_repos_from_db(self):
        '''Expands all repos in github_repos.db.ReposTodo table.'''

        if EXPAND_COST_GUESS > self.rate_limit_remaining * 100 or self.rate_limit_remaining <= 2:
            raise RateLimit()

        try:
            todo = self.session.query(ReposTodo).order_by(ReposTodo.id).first()

            log.info('Expanding %s/%s...', todo.repo.owner.login, todo.repo.name)
            sys.stdout.flush()

            # TODO handle weird timeout html page being returned
            result = send_query(EXPAND_QUERY, {'owner': todo.repo.owner.login, 'name': todo.repo.name})
            errors = result.get('errors', [])
            data = result['data']

            if errors:
                log.error(errors)
                for error in errors:
                    if 'type' in error and error['type'].lower() == 'rate_limited':
                        raise RateLimit()

            if not data:
                log.warning('result was empty')
                raise EmptyResultError(errors, todo)

            if isinstance(data, str):
                log.error('result was text, not a dictionary. Assuming Github timed out')
                raise GithubTimeout(errors, todo)

            repo_node = data.get('repository')
            if repo_node:
                repos = set()
                count = 0

                for key in ('mentionableUsers', 'stargazers', 'watchers'):
                    new_repos, new_count = get_repos_from_user_nodes(repo_node.get(key, {}).get('nodes', []))
                    repos.update(new_repos)
                    count += new_count

                new_count = 0
                # Create new owners and NewRepos
                for (owner_login, owner_type), repo_name in repos:
                    if not self.owner_exists(owner_login):
                        assert owner_type.lower() in ('user', 'organization')
                        owner = Owner(login=owner_login, type_id=self.session.query(OwnerType.id).filter_by(typename=owner_type).scalar())
                        self.session.add(owner)
                    else:
                        owner = self.session.query(Owner).filter_by(login=owner_login).one()

                    if not self.repo_exists(owner_login, repo_name):
                        self.session.add(NewRepo(owner=owner, name=repo_name))
                        new_count += 1

                log.info('%d repo nodes returned, %d unique, %d new', count, len(repos), new_count)

                # Delete todos
                self.session.query(ReposTodo).filter(ReposTodo.id == todo.id).delete(synchronize_session='fetch')
            else:
                log.warning('result was empty')
                raise EmptyResultError(errors, todo)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e

        finally:
            try:
                self.rate_limit_remaining = data['rateLimit']['remaining']
                rate_limit_reset_at = strp_reset_time(data['rateLimit']['resetAt'])
                actual_cost = data['rateLimit']['cost']
                self.session.add(QueryCost(guess=EXPAND_COST_GUESS, normalized_actual=actual_cost))

                self.reset_time = calendar.timegm(rate_limit_reset_at)
                local_reset_time_str = time.asctime(time.localtime(self.reset_time))

                log.info('Guessed cost %f, actual cost %d', EXPAND_COST_GUESS / 100, actual_cost)
                log.info('Rate limited cost %d remaining until %s', self.rate_limit_remaining, local_reset_time_str)
            except Exception as e:
                log.error(e)
                log.error('Could not get latest remaining query cost. `data` is probably None')


    def fetch_new_repo_info(self):
        if self.rate_limit_remaining <= 2:
            raise RateLimit()

        try:
            next_batch_size = max(0, min(500, int(self.rate_limit_remaining * 100) - 1))
            cost_guess = fetch_cost_guess(next_batch_size)
            todos = self.session.query(NewRepo).order_by(NewRepo.id).limit(next_batch_size).all()

            log.info('Fetching %d repos...', len(todos))
            sys.stdout.flush()

            repos_query = ''
            gensym_counter = 1
            for todo in todos:
                repos_query += '    repo%d: repository(owner: %s, name: %s) {...repoInfo}\n' % (
                    gensym_counter, json.dumps(todo.owner.login), json.dumps(todo.name))
                gensym_counter += 1

            query = FETCH_QUERY % repos_query

            result = send_query(query)
            errors = result.get('errors', [])
            data = result['data']

            if errors:
                log.error(errors)
                for error in errors:
                    if 'type' in error and error['type'].lower() == 'rate_limited':
                        raise RateLimit()

            if not data:
                log.warning('result was empty')
                raise EmptyResultError(errors, todos)

            if isinstance(data, str):
                log.error('result was text, not a dictionary. Assuming Github timed out')
                raise GithubTimeout(errors, todos)

            for key, node in data.items():
                if not key.lower().startswith('repo'):
                    continue

                for todo in todos:
                    if todo.name == node['name'] and todo.owner.login == node['owner']['login']:
                        self.session.delete(todo)

                repo = Repo(name=node['name'],
                            owner=self.session.query(Owner).filter_by(login=node['owner']['login']).one(),
                            description=node['description'],
                            disk_usage=node['diskUsage'],
                            url=node['url'],
                            is_fork=node['isFork'],
                            is_mirror=node['isMirror'])

                for lang_edge in node.get('languages', {}).get('edges', []):
                    try:
                        lang = self.session.query(Language).filter_by(name=lang_edge['node']['name']).one()
                    except NoResultFound:
                        lang = Language(name=lang_edge['node']['name'], color=lang_edge['node']['color'])

                    repo_lang = RepoLanguages(repo=repo, language=lang, bytes_used=lang_edge['size'])
                    self.session.add(repo_lang)

                self.session.add(repo)

                new_todo = ReposTodo(repo=repo)
                self.session.add(new_todo)

            log.info('done')

            self.rate_limit_remaining = data['rateLimit']['remaining']
            rate_limit_reset_at = strp_reset_time(data['rateLimit']['resetAt'])
            actual_cost = data['rateLimit']['cost']
            self.session.add(QueryCost(guess=cost_guess, normalized_actual=actual_cost))

            self.reset_time = calendar.timegm(rate_limit_reset_at)
            local_reset_time_str = time.asctime(time.localtime(self.reset_time))

            log.info('Guessed cost %f, actual cost %d', cost_guess / 100, actual_cost)
            log.info('Rate limited cost %d remaining until %s', self.rate_limit_remaining, local_reset_time_str)

            self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e


    def start(self):
        log.info('Starting scraper loop')
        log.info('Assuming %d rate limit cost remaining', self.rate_limit_remaining)
        log.info('Assuming rate limit reset time is %s', time.asctime(time.localtime(self.reset_time)))

        last_step_empty = False
        try:
            while True:
                # Expansion step
                if self.session.query(ReposTodo).first() is None:
                    log.info('No repos to expand. Skipping expansion step')
                    if last_step_empty:
                        return
                    last_step_empty = True
                else:
                    try:
                        self.expand_repos_from_db()

                    except RateLimit:
                        self.rate_limit_sleep()

                    except (GithubTimeout, EmptyResultError) as e:
                        todo = e.todo
                        self.expand_errors.setdefault(todo.id, [])
                        self.expand_errors[todo.id].append(e.errors)

                        errors_sofar = self.expand_errors[todo.id]
                        log.error('Error count for expanding %s/%s is %d',
                                  todo.repo.owner.login, todo.repo.name, len(errors_sofar))

                        if len(errors_sofar) > MAX_EXPAND_ERRORS:
                            with self.session.begin_nested():
                                self.session.add(RepoError(repo=todo.repo, error_text=repr([e for e in errors_sofar if e])))
                                self.session.delete(todo)
                            self.session.commit()

                    last_step_empty = False

                print()

                # Fetching step
                if self.session.query(NewRepo).first() is None:
                    log.info('No repos to fetch. Skipping fetching step')
                    if last_step_empty:
                        return
                    last_step_empty = True
                else:
                    try:
                        self.fetch_new_repo_info()

                    except RateLimit:
                        self.rate_limit_sleep()

                    except (GithubTimeout, EmptyResultError) as e:
                        todo = e.todo
                        self.fetch_errors += 1
                        log.error('Error count for fetching is %d', self.fetch_errors)

                        if self.fetch_errors > MAX_FETCH_ERRORS:
                            # Unacceptable!
                            raise MaxErrors()

                    last_step_empty = False

                print()
                print()

        except Exception as e:
            log.error(e)
            raise e

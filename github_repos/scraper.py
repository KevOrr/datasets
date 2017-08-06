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

MAX_EXPAND_ERRORS = 5
MAX_FETCH_ERRORS = 30
class MaxErrors(RuntimeError):
    pass

class GithubTimeout(RuntimeError):
    def __init__(self, todo):
        RuntimeError.__init__(self)
        self.todo = todo

class EmptyResultError(RuntimeError):
    def __init__(self, todo):
        RuntimeError.__init__(self)
        self.todo = todo

class MainScraper():
    def __init__(self):
        self.session = Session()
        self.rate_limit_remaining = 5000
        self.last_step_empty = False
        self.expand_errors = {}
        self.fetch_errors = 0
        self.reset_time = time.time() + 3600

    def rate_limit_sleep(self):
        now = time.time()
        sleep_time = 10 + max(0, self.reset_time - now)
        log.info('Sleeping %d seconds until %s', sleep_time, time.asctime(time.localtime(self.reset_time)))
        time.sleep(sleep_time)


    def do_scrape_step(self, fun, *args, **kwargs):
        try:
            self.rate_limit_remaining, self.reset_time = fun(*args, **kwargs)
            if self.rate_limit_remaining <= 2:
                self.rate_limit_sleep()

        except RateLimit:
            self.rate_limit_sleep()

        except NoResultFound as e:
            if self.last_step_empty:
                raise e
            self.last_step_empty = True

        except (GithubTimeout, EmptyResultError) as e:
            todo = e.todo
            if isinstance(todo, list):
                self.fetch_errors += 1
                log.error('Error count for fetching is %d', self.fetch_errors)

                if self.fetch_errors > MAX_FETCH_ERRORS:
                    # Unacceptable!
                    raise MaxErrors()

            elif isinstance(todo, ReposTodo):
                self.expand_errors.setdefault(todo.id, 0)
                self.expand_errors[todo.id] += 1
                log.error('Error count for expanding %s/%s is %d',
                          todo.repo.owner.login, todo.repo.name, self.expand_errors[todo.id])

                if self.expand_errors[todo.id] > MAX_EXPAND_ERRORS:
                    with self.session.begin_nested():
                        self.session.add(RepoError(repo=todo.repo))
                        self.session.delete(todo)
                    self.session.commit()

        else:
            self.last_step_empty = False
            if fun is fetch_new_repo_info:
                self.fetch_errors = max(0, self.fetch_errors - 1)

        print()

    def start(self):
        log.info('Starting scraper loop')
        log.info('Assuming %d rate limit cost remaining', self.rate_limit_remaining)
        log.info('Assuming rate limit reset time is %s', time.asctime(time.localtime(self.reset_time)))

        while True:
            self.do_scrape_step(expand_repos_from_db, self.session, self.rate_limit_remaining)
            self.do_scrape_step(fetch_new_repo_info, self.session, self.rate_limit_remaining)

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

class GraphQLException(RuntimeError):
    pass

class RateLimit(RuntimeError):
    pass

def send_query(query, variables=None, url=g.api_url, api_key=g.personal_token, sender=requests.post):
    if not isinstance(query, str):
        query = query.format()

    headers = {'Authorization': 'bearer ' + api_key}
    if variables:
        result = sender(url, headers=headers, data=json.dumps({'query': query, 'variables': variables})).json()
    else:
        result = sender(url, headers=headers, data=json.dumps({'query': query})).json()

    return result


def owner_exists(session, login):
    return session.query(exists().where(Owner.login == login)).scalar()

def repo_fetched(session, owner_login, repo_name):
    return session.query(
        session.query(Owner).filter_by(login=owner_login).join(Repo).filter_by(name=repo_name).exists()
    ).scalar()

def repo_found_not_fetched(session, owner_login, repo_name):
    return session.query(
        session.query(Owner).filter_by(login=owner_login).join(NewRepo).filter(NewRepo.name == repo_name).exists()
    ).scalar()

def repo_exists(session, owner_login, repo_name):
    return repo_fetched(session, owner_login, repo_name) or repo_found_not_fetched(session, owner_login, repo_name)


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


def expand_repos_from_db(session, rate_limit_remaining=5000):
    '''Expands all repos in github_repos.db.ReposTodo table.'''

    if EXPAND_COST_GUESS > rate_limit_remaining * 100 or rate_limit_remaining <= 2:
        raise RateLimit()

    try:
        todo = session.query(ReposTodo).order_by(ReposTodo.id).first()

        log.info('Expanding %s/%s...', todo.repo.owner.login, todo.repo.name)
        sys.stdout.flush()

        # TODO handle weird timeout html page being returned
        result = send_query(EXPAND_QUERY, {'owner': todo.repo.owner.login, 'name': todo.repo.name})
        errors = result.get('errors', [])
        data = result['data']

        if errors:
            log.error(errors)

        if not data:
            log.warning('result was empty')
            raise EmptyResultError(todo)

        if isinstance(data, str):
            log.error('result was text, not a dictionary. Assuming Github timed out')
            raise GithubTimeout(todo)

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
                if not owner_exists(session, owner_login):
                    assert owner_type.lower() in ('user', 'organization')
                    owner = Owner(login=owner_login, type_id=session.query(OwnerType.id).filter_by(typename=owner_type).scalar())
                    session.add(owner)
                else:
                    owner = session.query(Owner).filter_by(login=owner_login).one()

                if not repo_exists(session, owner_login, repo_name):
                    session.add(NewRepo(owner=owner, name=repo_name))
                    new_count += 1

            log.info('%d repo nodes returned, %d unique, %d new', count, len(repos), new_count)

            # Delete todos
            session.query(ReposTodo).filter(ReposTodo.id == todo.id).delete(synchronize_session='fetch')

        rate_limit_remaining = data['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
        actual_cost = data['rateLimit']['cost']
        session.add(QueryCost(guess=EXPAND_COST_GUESS, normalized_actual=actual_cost))

        reset_time = calendar.timegm(rate_limit_reset_at)
        local_reset_time_str = time.asctime(time.localtime(reset_time))

        log.info('Guessed cost %f, actual cost %d', EXPAND_COST_GUESS / 100, actual_cost)
        log.info('Rate limited cost %d remaining until %s', rate_limit_remaining, local_reset_time_str)

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    return rate_limit_remaining, reset_time

def fetch_new_repo_info(session, rate_limit_remaining=5000):
    if rate_limit_remaining <= 2:
        raise RateLimit()

    try:
        next_batch_size = max(0, min(500, int(rate_limit_remaining * 100) - 1))
        cost_guess = fetch_cost_guess(next_batch_size)
        todos = session.query(NewRepo).order_by(NewRepo.id).limit(next_batch_size).all()

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

        if not data:
            log.warning('result was empty')
            raise EmptyResultError(todos)

        if isinstance(data, str):
            log.error('result was text, not a dictionary. Assuming Github timed out')
            raise GithubTimeout(todos)

        for key, node in data.items():
            if not key.lower().startswith('repo'):
                continue

            for todo in todos:
                if todo.name == node['name'] and todo.owner.login == node['owner']['login']:
                    session.delete(todo)

            repo = Repo(name=node['name'],
                        owner=session.query(Owner).filter_by(login=node['owner']['login']).one(),
                        description=node['description'],
                        disk_usage=node['diskUsage'],
                        url=node['url'],
                        is_fork=node['isFork'],
                        is_mirror=node['isMirror'])

            for lang_edge in node.get('languages', {}).get('edges', []):
                try:
                    lang = session.query(Language).filter_by(name=lang_edge['node']['name']).one()
                except NoResultFound:
                    lang = Language(name=lang_edge['node']['name'], color=lang_edge['node']['color'])

                repo_lang = RepoLanguages(repo=repo, language=lang, bytes_used=lang_edge['size'])
                session.add(repo_lang)

            session.add(repo)

            new_todo = ReposTodo(repo=repo)
            session.add(new_todo)

        log.info('done')

        rate_limit_remaining = data['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
        actual_cost = data['rateLimit']['cost']
        session.add(QueryCost(guess=cost_guess, normalized_actual=actual_cost))

        reset_time = calendar.timegm(rate_limit_reset_at)
        local_reset_time_str = time.asctime(time.localtime(reset_time))

        log.info('Guessed cost %f, actual cost %d', cost_guess / 100, actual_cost)
        log.info('Rate limited cost %d remaining until %s', rate_limit_remaining, local_reset_time_str)

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    return rate_limit_remaining, reset_time

import json
import time
import calendar
import sys

import requests
from sqlalchemy import exists
from sqlalchemy.orm.exc import NoResultFound

from github_repos.db import Repo, NewRepo, ReposTodo, Owner, OwnerType
from github_repos.db import RepoLanguages, Language, QueryCost
import github_repos.config as g


EXPAND_COST_GUESS = 18484
EXPAND_NODES_GUESS = 38688
EXPAND_QUERY = '''\
query($owner:String!, $name:String!) {
    repository(owner: $owner, name: $name) {
        mentionableUsers(first: 10) {
            nodes {
                ...userExpand
            }
        }
        pullRequests(first: 10) {
            nodes {
                participants(first: 2) {
                    nodes {
                        ...userExpand
                    }
                }
            }
        }
        stargazers(first: 100, orderBy: {field: STARRED_AT, direction: DESC}) {
            nodes {
                ...userExpand
            }
        }
        watchers(first: 100) {
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
    issues(first: 10, orderBy: {field: COMMENTS, direction: DESC}) {
        nodes {
            repository {
                ...repoInfo
            }
        }
    }
    pullRequests(first: 10) {
        nodes {
            repository {
                ...repoInfo
            }
        }
    }
    starredRepositories(first: 10, orderBy: {field: STARRED_AT, direction: DESC}) {
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

fetch_cost_guess = lambda n: 12*n + 1
fetch_nodes_guess = lambda n: 51*n + 5
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

    for key in ('contributedRepositories', 'starredRepositories', 'issues', 'pullRequests'):
        for user in user_nodes:
            for node in user.get(key, {}).get('nodes', []):
                try:
                    repo = node if key in ('contributedRepositories', 'starredRepositories') else node['repository']
                    repos.add(((repo['owner']['login'], repo['owner']['__typename']), repo['name']))
                except KeyError:
                    pass

    return repos


def expand_repos_from_db(session, rate_limit_remaining=5000):
    '''Expands all repos in github_repos.db.ReposTodo table.'''

    try:
        todo = session.query(ReposTodo).order_by(ReposTodo.id).first()

        print('Expanding {}/{}...'.format(todo.repo.owner.login, todo.repo.name), end=' ')
        sys.stdout.flush()

        # TODO handle weird timeout html page being returned
        result = send_query(EXPAND_QUERY, {'owner': todo.repo.owner.login, 'name': todo.repo.name})
        errors = result.get('errors', [])
        data = result['data']

        if errors and data:
            print(errors)
        elif not data:
            raise GraphQLException(result['errors'])

        repo_node = data.get('repository')
        if repo_node:
            repos = set()

            repos.update(get_repos_from_user_nodes(repo_node.get('mentionableUsers', {}).get('nodes', [])))
            repos.update(get_repos_from_user_nodes(repo_node.get('stargazers', {}).get('nodes', [])))
            repos.update(get_repos_from_user_nodes(repo_node.get('watchers', {}).get('nodes', [])))
            for pr in repo_node.get('pullRequests', {}).get('nodes', []):
                repos.update(get_repos_from_user_nodes(pr.get('participants', {}).get('nodes', [])))

            print('{} new repos found'.format(len(repos)))

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

            # Delete todos
            session.query(ReposTodo).filter(ReposTodo.id == todo.id).delete(synchronize_session='fetch')

        rate_limit_remaining = data['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
        actual_cost = data['rateLimit']['cost']
        session.add(QueryCost(guess=EXPAND_COST_GUESS, normalized_actual=actual_cost))

        reset_time = calendar.timegm(rate_limit_reset_at)
        local_reset_time_str = time.asctime(time.localtime(reset_time))

        print('Guessed cost {}, actual cost {}'.format(EXPAND_COST_GUESS / 100, actual_cost))
        print('Rate limited cost {} remaining until {}'.format(rate_limit_remaining, local_reset_time_str))
        print()

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    return rate_limit_remaining, reset_time, local_reset_time_str

def fetch_new_repo_info(session, rate_limit_remaining=5000):
    per_repo_cost = 12
    try:
        next_batch_size = int(rate_limit_remaining * 100 / (1 + per_repo_cost)) - 1
        cost_guess = fetch_cost_guess(next_batch_size)
        todos = session.query(NewRepo).order_by(NewRepo.id).limit(next_batch_size).all()

        print('Fetching {} repos...'.format(len(todos)), end=' ')
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

        if errors and data:
            print(errors)
        elif not data:
            raise GraphQLException(result['errors'])

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

        print('done')

        rate_limit_remaining = data['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
        actual_cost = data['rateLimit']['cost']
        session.add(QueryCost(guess=cost_guess, normalized_actual=actual_cost))

        reset_time = calendar.timegm(rate_limit_reset_at)
        local_reset_time_str = time.asctime(time.localtime(reset_time))

        print('Guessed cost {}, actual cost {}'.format(cost_guess / 100, actual_cost))
        print('Rate limited cost {} remaining until {}'.format(rate_limit_remaining, local_reset_time_str))
        print()

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    return rate_limit_remaining, reset_time, local_reset_time_str

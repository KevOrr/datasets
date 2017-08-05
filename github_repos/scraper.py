import itertools as it
import json
import time
import calendar
from textwrap import dedent

import requests
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import exists

from github_repos.graphql import Node as Gqn
from github_repos.db import Session
from github_repos.db import Repo, NewRepo, ReposTodo, Owner
from github_repos.db import repo_languages, Language, QueryCost
import github_repos.config as g

EXPAND_QUERY = '''\
query {
    repository(owner: $owner, name: $name){
        ...repoExpand
    }
    rateLimit {
        cost
        remaining
        resetAt
    }
}

fragment repoInfo on Repository {
    name
    owner {
        __typename
        login
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

fragment repoExpand on Repository {
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
}'''

FETCH_QUERY = '''\
query {
    repository(owner: $owner, name: $name) {
        ...repoInfo
    }
    rateLimit {
        cost
        remaining
        resetAt
    }
}

fragment repoInfo on Repository {
    decription
    diskUsage
    url
    isFork
    isMirror
}'''

class GraphQLException(RuntimeError):
    pass

def send_query(query, variables, url=g.api_url, api_key=g.personal_token, sender=requests.post):
    if not isinstance(query, str):
        query = query.format()

    headers = {'Authorization': 'bearer ' + api_key}
    result = sender(url, headers=headers, data=json.dumps({'query': query, 'variables': variables})).json()

    return result


def owner_exists(session, login):
    return session.query(exists().where(Owner.login == login)).scalar()

def repo_exists(session, owner_login, repo_name):
    return session.query(exists().where((Repo.name == repo_name) & (Repo.owner.login == owner_login)))

def expand_repos_from_db(session, rate_limit_remaining=5000):

    '''Expands all repos in github_repos.db.ReposTodo table.'''

    cost_guess = 2200
    nodes_guess = 51774
    try:
        todo = session.query(ReposTodo).order_by(ReposTodo.id).one()

        # TODO handle weird timeout html page being returned
        result = send_query(EXPAND_QUERY, {'owner': todo.repo.owner.login, 'name': todo.repo.name})

        errors = result.get('errors', [])
        if errors:
            print(errors)

        data = result['data']

        repo_node = data.get('repository')
        if repo_node:
            repos = set()

            repos.update(get_repos_from_user_nodes(repo_node.get('mentionableUsers', {}).get('nodes', [])))
            repos.update(get_repos_from_user_nodes(repo_node.get('stargazers', {}).get('nodes', [])))
            repos.update(get_repos_from_user_nodes(repo_node.get('watchers', {}).get('nodes', [])))
            for pr in repo_node.get('pullRequests', {}).get('nodes', []):
                repos.update(get_repos_from_user_nodes(pr.get('participants', {}).get('nodes', [])))

            # Create new owners and NewRepos
            for (owner_login, owner_type), repo_name in repos:
                if not owner_exists(session, owner_login):
                    assert owner_type.lower() in ('user', 'organization')
                    owner = Owner(login=owner_login, owner_typename=owner_type)
                    session.add(owner)
                else:
                    owner = session.query(Owner).filter_by(login=owner_login).one()

                if not repo_exists(session, owner_login, repo_name):
                    session.add(NewRepo(owner=owner, name=repo_name))

            # Delete todos
            session.query(ReposTodo).filter(ReposTodo.id == todo_id).delete(synchronize_session='fetch')

        rate_limit_remaining = data['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
        session.add(QueryCost(guess=cost_guess, normalized_actual=result['rateLimit']['cost']))

        if rate_limit_remaining <= 2:
            reset_time = calendar.timegm(rate_limit_reset_at)
            now = time.gmtime()
            time.sleep(10 + max(0, reset_time - now))

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

def fetch_new_repo_info(session, rate_limit_remaining):
    per_repo_cost = 1
    try:
        next_batch_size = int(rate_limit_remaining * 100 / (1 + 100*per_repo_cost)) - 1
        todos_ids, todos_repo_ids = tuple(zip(*session.query(ReposTodo.id, ReposTodo.repo_id)
                                              .order_by(ReposTodo.id).limit(next_batch_size).all()))
        repos = session.query(Repo.owner.login, Repo.name).filter(Repo.id.in_(todos_repo_ids)).all()
    except:
        pass


def expand_all_repos(engine, rate_limit_remaining=5000,
                     # expand_contributors=g.scraper_expand_repo_contributors,
                     expand_issues=g.scraper_expand_repo_issues_participants,
                     expand_pullrequests=g.scraper_expand_repo_pullrequest_participants,
                     expand_stars=g.scraper_expand_repo_stars,
                     expand_watchers=g.scraper_expand_repo_watchers):

    '''Expands all repos in github_repos.db.ReposTodo table.

    expand_issues           whether or not to find users based from each repo's issues
    expand_pullrequests     whether or not to find users based from each repo's pullrequests
    expand_stars            whether or not to find visit each repo's stargazer
    expand_watchers         whether or not to find visit each repo's watcher
    '''
    pass

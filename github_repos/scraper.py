import itertools as it
import json
import time
import calendar
from textwrap import dedent

import requests
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import exists

from github_repos.graphql import Node as Gqn
from github_repos.db import associations, Session
from github_repos.db import User, Repo, Language, QueryCost
from github_repos.db import UsersTodo, ReposTodo, NewRepo
import github_repos.config as g
from github_repos.util import count_bools

class GraphQLException(RuntimeError):
    pass

def send_query(query, url=g.api_url, api_key=g.personal_token, sender=requests.post):
    if not isinstance(query, str):
        query = query.format()

    headers = {'Authorization': 'bearer ' + api_key}
    result = sender(url, headers=headers, data=json.dumps({'query': query})).json()

    return result

def both_steps_format(repos):
    fragments = dedent('''
    fragment repoInfo on Repository {
        name
        owner {
            __typename
            login
        }
    }

    fragment userConnectionExpand on User {
        nodes {
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
    }

    fragment repoExpand on Repository {
        mentionableUsers(first: 10) {
            ...userConnectionExpand
        }
        pullRequests(first: 10) {
            participants {
                ...userConnectionExpand
            }
        }
        starGazers(first: 100, orderBy: {field: STARRED_AT, direction: DESC}) {
            ...userConnectionExpand
        }
        watchers(first: 100) {
            ...userConnectionExpand
        }
    }''')

    rate_limit = '\n    rateLimit {cost remaining resetAt}'''

    gensym_counter = 1
    query = ''
    for owner, name in repos:
        query += '    repo%d: repository(owner:%s, name:%s){...repoExpand}\n' % \
                 (gensym_counter, json.dumps(owner), json.dumps(name))
        gensym_counter += 1

    query = 'query {\n' + query + rate_limit + '\n}\n' + fragments
    cost_guess = 2200*len(repos) + 1

    return (query, cost_guess)

def format_user_expander(user_logins,
                         contributions_cursors=None,
                         issues_cursors=None,
                         pullrequests_cursors=None,
                         expand_contributions=g.scraper_expand_user_contributions,
                         expand_issues=g.scraper_expand_user_issues,
                         expand_pullrequests=g.scraper_expand_user_pullrequests):

    '''Returns a github_repos.graphql.GraphQLNode that expands all users in `user_logins`.

    user_logins             a list of login strings to visit
    contributions_cursors    start at the cursor returned by User.contributedRepositories
    issues_cursors           start at the cursor returned by User.issues
    pullrequests_cursors     start at the cursor returned by User.pullRequests
    expand_contributions    whether or not to find repos based on each user's associated contributions
    expand_issues           whether or not to find repos based on each user's associated issues
    expand_pullrequests     whether or not to find repos based on each user's associated pullrequests
    '''

    if not contributions_cursors:
        contributions_cursors = it.repeat(None)

    if not issues_cursors:
        issues_cursors = it.repeat(None)

    if not pullrequests_cursors:
        pullrequests_cursors = it.repeat(None)

    gensym_counter = 1
    query = ''
    for user, cc, ic, pc in zip(user_logins, contributions_cursors, issues_cursors, pullrequests_cursors):
        sub_nodes = ''

        if expand_contributions:
            sub_nodes += 'contributedRepositories(first: 100, after: %s) {...ownerOfNode}\n' % json.dumps(cc)

        if expand_issues:
            sub_nodes += 'issues(first: 100, after: %s) {...ownerOfRepoOfNode}\n' % json.dumps(ic)

        if expand_pullrequests:
            sub_nodes += 'pullRequests(first: 100, after: %s) {...ownerOfRepoOfNode}\n' % json.dumps(pc)

        query += 'usr%d: user(login=%s){%s}\n' % (gensym_counter, json.dumps(user), sub_nodes)
        gensym_counter += 1

    query += '''
    fragment ownerOfNode on User {
        nodes {
            owner {
                __typename
                login
            }
            name
        }
    }

    fragment ownerOfRepoOfNode on User {
        nodes {
            repository {
                owner {
                    __typename
                    login
                }
                name
            }
        }
    }'''

    per_user_cost = count_bools((expand_contributions, expand_issues, expand_pullrequests))
    cost = len(user_logins) * (1 + 100*per_user_cost)

    print(query)

    return (query, cost)

def format_repo_expander(unique_repos,

                         # contributors_cursors=None,
                         issues_cursors=None,
                         pullrequests_cursors=None,
                         stars_cursors=None,
                         watchers_cursors=None,
                         # expand_contributors=g.scraper_expand_repo_contributors,
                         expand_issues=g.scraper_expand_repo_issues_participants,
                         expand_pullrequests=g.scraper_expand_repo_pullrequest_participants,
                         expand_stars=g.scraper_expand_repo_stars,
                         expand_watchers=g.scraper_expand_repo_watchers):

    '''Returns a github_repos.graphql.GraphQLNode that expands all repos in `users_and_repos`.

    unique_repos            a list of 2-tuples for each user/repo pair (unique repos)
    issues_cursors           start at the cursor returned by Repository.issues
    pullrequests_cursors     start at the cursor returned by Repository.pullRequests
    stars_cursors            start at the cursor returned by Repository.stargazers
    watchers_cursors         start at the cursor returned by Repository.watchers
    expand_issues           whether or not to find users based from each repo's issues
    expand_pullrequests     whether or not to find users based from each repo's pullrequests
    expand_stars            whether or not to find visit each repo's stargazer
    expand_watchers         whether or not to find visit each repo's watcher
    '''

    if not issues_cursors:
        issues_cursors = it.repeat(None)

    if not pullrequests_cursors:
        pullrequests_cursors = it.repeat(None)

    if not stars_cursors:
        stars_cursors = it.repeat(None)

    if not watchers_cursors:
        watchers_cursors = it.repeat(None)

    nodes = []
    for (user, repo), ic, pc, sc, wc in zip(unique_repos, issues_cursors, pullrequests_cursors,
                                            stars_cursors, watchers_cursors):
        sub_nodes = []

        # TODO API v4 so far has no way to get repository contributors
        # if expand_contributors:
        #     sub_nodes.append(
        #         Gqn('contributors',
        #             first=100,
        #             after=cc)(
        #                 Gqn('nodes')(
        #                     Gqn('login'))))

        if expand_issues:
            sub_nodes.append(
                Gqn('issues',
                    first=100,
                    after=ic)(
                        Gqn('nodes')(
                            Gqn('login'))))

        if expand_pullrequests:
            sub_nodes.append(
                Gqn('pullRequests',
                    first=100,
                    after=pc)(
                        Gqn('nodes')(
                            Gqn('login'))))

        if expand_stars:
            sub_nodes.append(
                Gqn('stargazers',
                    first=100,
                    after=sc)(
                        Gqn('nodes')(
                            Gqn('login'))))

        if expand_watchers:
            sub_nodes.append(
                Gqn('watchers',
                    first=100,
                    after=wc)(
                        Gqn('nodes')(
                            Gqn('login'))))

        nodes.append(Gqn('repository', owner=user, name=repo)(*sub_nodes))

    query = \
            Gqn('query')(
                Gqn('rateLimit')(
                    Gqn('cost'),
                    Gqn('remaining'),
                    Gqn('resetAt')),
                *nodes)

    per_repo_cost = count_bools((expand_issues, expand_pullrequests, expand_stars, expand_watchers))
    cost = len(unique_repos) * (1 + 100*per_repo_cost)

    return (query, cost)


def user_discovered_fetched(session, login):
    return session.query(exists().where(User.login == login)).scalar()

def repo_discovered_not_fetched(session, owner_login, name):
    return session.query(exists().where((NewRepo.owner_login == owner_login) & (NewRepo.name == name))).scalar()

def repo_fetched(session, owner_login, name):
    return session.query(exists().where((Repo.owner.login == owner_login) & (Repo.name == name))).scalar()


def fetch_owners(session, owners):
    nodes = []
    gensym_counter = 1
    for owner in owners:
        nodes.append(Gqn('usr' + str(gensym_counter), 'user', login=owner.login)(
            Gqn('name')))
        gensym_counter += 1

    query = \
            Gqn('query')(
                Gqn('rateLimit')(
                    Gqn('cost'),
                    Gqn('remaining'),
                    Gqn('resetAt')),
                *nodes)

    return send_query(query)

def expand_all_users(session, rate_limit_remaining=5000,
                     contributions_cursors=None,
                     issues_cursors=None,
                     pullrequests_cursors=None,
                     expand_contributions=g.scraper_expand_user_contributions,
                     expand_issues=g.scraper_expand_user_issues,
                     expand_pullrequests=g.scraper_expand_user_pullrequests):

    '''Expands all users in github_repos.db.UsersTodo table.

    expand_contributions    whether or not to find repos based on each user's associated contributions
    expand_issues           whether or not to find repos based on each user's associated issues
    expand_pullrequests     whether or not to find repos based on each user's associated pullrequests
    '''

    per_user_cost = count_bools((expand_contributions, expand_issues, expand_pullrequests))
    contributions_cursors = None
    issues_cursors = None
    pullrequests_cursors = None
    try:
        next_batch_size = int(rate_limit_remaining * 100 / (1 + 100*per_user_cost)) - 1
        todos_ids = tuple(t[0] for t in session.query(UsersTodo.user_id).order_by(UsersTodo.id).limit(next_batch_size).all())
        # TODO figure this out
        user_logins = tuple(u[0] for u in session.query(User.login).filter(User.id.in_(todos_ids)).all())

        query, cost_guess = format_user_expander(user_logins,
                                                 contributions_cursors=contributions_cursors,
                                                 issues_cursors=issues_cursors,
                                                 pullrequests_cursors=pullrequests_cursors)

        # TODO handle weird timeout html page being returned
        import pdb; pdb.set_trace()
        result = send_query(query)

        repos = []
        owners_to_fetch = set()
        for data_key, user in result.items():
            if not data_key.lower().startswith('usr'):
                # Not at a user node (probably rateLimit)
                continue

            for connection in ('contributedRepositories', 'issues', 'pullRequests'):
                for node in user[connection]['nodes']:
                    if connection == 'contributedRepositories':
                        repo = node
                    else:
                        repo = node['repository']

                    owner_login = repo['owner']['login']
                    if user_discovered_fetched(session, owner_login):
                        owner = session.query(User).filter_by(login=owner_login).one()
                    else:
                        owner = User(login=owner_login)
                        owners_to_fetch.add(owner)
                        session.add(owner)

                    repos.append((owner, repo['name']))

        import pdb; pdb.set_trace()
        # TODO handle weird timeout html page being returned
        fetch_owners_result = fetch_owners(session, owners_to_fetch)
        rate_limit_remaining = fetch_owners_result['rateLimit']['remaining']
        rate_limit_reset_at = time.strptime(fetch_owners_result['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")

        session.add(QueryCost(guess=cost_guess, normalized_actual=result['rateLimit']['cost']))

        # Keep the delete-todos-and-insert-new cycle as short as possible, then commit
        session.query(UsersTodo).filter(UsersTodo.id.in_(todos_ids)).delete(synchronize_session='fetch')
        for owner, name in repos:
            repo = NewRepo(owner_login=owner.login, name=name)
            session.add(repo)

        if rate_limit_remaining <= 2:
            reset_time = calendar.timegm(rate_limit_reset_at)
            now = time.gmtime()
            time.sleep(10 + max(0, reset_time - now))

        session.commit()

    except Exception as e:
        session.rollback()
        raise e


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

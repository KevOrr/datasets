import itertools as it
import json
import time
import calendar

import requests
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import exists

from github_repos.graphql import GraphQLNode as Gqn
from github_repos.db import associations, Session
from github_repos.db import User, Repo, Language, QueryCost
from github_repos.db import UsersTodo, ReposTodo, NewUser, NewRepo
import github_repos.config as g
from github_repos.util import count_bools

class GraphQLException(RuntimeError):
    pass

def send_query(query, url=g.api_url, api_key=g.personal_token, sender=requests.post):
    headers = {'Authorization': 'bearer ' + api_key}
    return sender(url, headers=headers, data=json.dumps({'query': query.format()})).json()


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

    nodes = []
    for user, cc, ic, pc in zip(user_logins, contributions_cursors, issues_cursors, pullrequests_cursors):
        sub_nodes = []

        if expand_contributions:
            sub_nodes.append(
                Gqn('contributedRepositories',
                    first=100,
                    after=cc)(
                        Gqn('nodes')(
                            Gqn('owner')(
                                Gqn('login')),
                            Gqn('name'))))

        if expand_issues:
            sub_nodes.append(
                Gqn('issues',
                    first=100,
                    after=ic)(
                        Gqn('nodes')(
                            Gqn('owner')(
                                Gqn('login')),
                            Gqn('name'))))

        if expand_pullrequests:
            sub_nodes.append(
                Gqn('pullRequests',
                    first=100,
                    after=pc)(
                        Gqn('nodes')(
                            Gqn('owner')(
                                Gqn('login')),
                            Gqn('name'))))

        nodes.append(Gqn(user, 'user', login=user)(*sub_nodes))

    query = \
            Gqn('query')(
                Gqn('rateLimit')(
                    Gqn('cost'),
                    Gqn('remaining'),
                    Gqn('resetAt')),
                *nodes)

    per_user_cost = count_bools((expand_contributions, expand_issues, expand_pullrequests))
    cost = len(user_logins) * (1 + 100*per_user_cost)

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


def user_discovered_not_fetched(session, login):
    return session.query(exists().where(NewUser.login == login)).scalar()

def user_fetched(session, login):
    return session.query(exists().where(User.login == login)).scalar()

def repo_discovered_not_fetched(session, owner_login, name):
    return session.query(exists().where((NewRepo.owner_login == owner_login) & (NewRepo.name == name))).scalar()

def repo_fetched(session, owner_login, name):
    return session.query(exists().where((Repo.owner.login == owner_login) & (Repo.name == name))).scalar()


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
        todos = session.query(UsersTodo).order_by(UsersTodo.id).limit(next_batch_size).subquery()
        user_logins = session.query(User.login).join(todos)

        query, cost_guess = format_user_expander(user_logins,
                                                 contributions_cursors=contributions_cursors,
                                                 issues_cursors=issues_cursors,
                                                 pullrequests_cursors=pullrequests_cursors)

        try:
            result = send_query(query)
        except json.JSONDecodeError as e:
            # TODO handle weird timeout html page being returned
            raise e

        if 'errors' in result:
            raise GraphQLException(result['error'])

        data = result['data']

        repos = []
        for user_dict in data['user']:
            for key in ('contributedRepositories', 'issues', 'pullRequests'):
                for repo in user_dict[key].get('nodes', ()):
                    owner_login = repo['user']['login']
                    if user_fetched(session, owner_login):
                        owner = session.query(User).filter_by(login=owner_login).one()
                    elif user_discovered_not_fetched(session, owner_login):
                        owner = session.query(NewUser).filter_by(login=owner_login).one()
                    else:
                        owner = NewUser(login=owner_login)
                        session.add(owner)

                    repos.append(owner, repo['name'])

        session.commit()

        # Keep the delete-todos-and-insert-new cycle as short as possible, then commit
        session.delete_all(todos)
        for owner, name in repos:
            repo = Repo(owner_id=owner.id, name=name)
            session.add(repo)

        session.add(QueryCost(guess=cost_guess, normalized_actual=data['rateLimit']['cost']))

        rate_limit_remaining = data['rateLimit']['remaining']
        if rate_limit_remaining <= 2:
            rate_limit_reset_at = time.strptime(data['rateLimit']['resetAt'], "%Y-%m-%dT%H:%M:%Sz")
            reset_time = calendar.timegm(rate_limit_reset_at)
            now = time.gmtime()

            time.sleep(10 + max(0, reset_time - now))

    except Exception as e:
        session.rollback()
        raise e
    else:
        session.commit()


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

import itertools as it

from github_repos.graphql import GraphQLNode as Gqn
from github_repos.db import User, Repo, Language, associations
import github_repos.config as g
from github_repos.util import count_bools

# REPO_INFO = Gqn()

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
                        Gqn('edges')(
                            Gqn('nodes'))))

        if expand_issues:
            sub_nodes.append(
                Gqn('issues',
                    first=100,
                    after=ic)(
                        Gqn('edges')(
                            Gqn('nodes'))))

        if expand_pullrequests:
            sub_nodes.append(
                Gqn('pullRequests',
                    first=100,
                    after=pc)(
                        Gqn('edges')(
                            Gqn('nodes'))))

        nodes.append(Gqn('user', login=user)(*sub_nodes))

    query =  Gqn('query')(*nodes)

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

    query = Gqn('query')(*nodes)

    per_repo_cost = count_bools((expand_issues, expand_pullrequests, expand_stars, expand_watchers))
    cost = len(unique_repos) * (1 + 100*per_repo_cost)

    return (query, cost)


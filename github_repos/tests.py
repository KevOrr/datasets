import unittest

from github_repos.graphql import GraphQLNode as gqn
from github_repos.scraper import send_query

class TestQuerying(unittest.TestCase):
    def test_query_sending(self):
        query = \
                gqn('query')(
                    gqn('user', login='syl20bnr')(
                        gqn('contributedRepositories', first=100)(
                            gqn('nodes')(
                                gqn('owner')(
                                    gqn('login')),
                                gqn('name')))))

        response = send_query(query)
        self.assertIn('data', response)
        self.assertIn('user', response['data'])
        self.assertIn('contributedRepositories', response['data']['user'])
        self.assertIn('nodes', response['data']['user']['contributedRepositories'])

        for repo in response['data']['user']['contributedRepositories']['nodes']:
            self.assertIn('name', repo)
            self.assertIn('owner', repo)
            self.assertIn('login', repo['owner'])

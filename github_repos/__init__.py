import github_repos.scraper as scraper
import github_repos.graphql as graphql
import github_repos.config as config

if config.debug:
    import importlib
    importlib.reload(config)
    importlib.reload(graphql)
    importlib.reload(scraper)
    del importlib

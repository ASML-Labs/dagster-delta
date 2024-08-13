### Steps to contribute:

1. Install python minimum python 3.9, with pyenv for example.
2. Run ```make .venv``` for local venv, then ```source .venv/bin/activate```
3. Linting, formatting, typechecking with ```make pre-commit```
4. Run tests with ```pytest .```


### Pull requests

When you have resolved your issue, [open a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) in the dagster-delta repository.
Please adhere to the following guidelines:

- Start your pull request title with a [conventional commit](https://www.conventionalcommits.org/) tag. (chore, feature, fix)
- Add any relevant information to the description that you think may help others review your code.
- Make sure your branch is [rebased](https://docs.github.com/en/get-started/using-git/about-git-rebase) against the latest version of the `main` branch.
- Make sure all github CI Checks pass.
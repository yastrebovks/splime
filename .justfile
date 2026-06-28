project_name := 'spl'
python_version := '3.13'

default:
  @just --justfile {{justfile()}} --list


dev env_name='dev': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  uv run --active -m ipykernel install \
    --user \
    --name='python_{{snakecase(project_name)}}' \
    --display-name='Python ({{kebabcase(project_name)}} env)'


[group('testing(static)')]
lint env_name='test': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  RUFF_SELECT='B,E,F,I,N,W,ICN,INP,S,T20,RET,RUF,PTH'
  RUFF_IGNORE='E251,E701,E731,E741'

  uvx ruff check --select "$RUFF_SELECT" --ignore "$RUFF_IGNORE" ./src
  uvx ruff check --select "$RUFF_SELECT" --ignore "$RUFF_IGNORE" ./tests


[group('testing(static)')]
lint-fix env_name='test': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  RUFF_SELECT='B,E,F,I,N,W,ICN,INP,S,T20,RET,RUF,PTH'
  RUFF_IGNORE='E251,E701,E731,E741'

  uvx ruff check --select "$RUFF_SELECT" --ignore "$RUFF_IGNORE" --fix ./src
  uvx ruff check --select "$RUFF_SELECT" --ignore "$RUFF_IGNORE" --fix ./tests


[group('testing(static)')]
typecheck env_name='test': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  RUFF_SELECT='PYI,TC'

  uvx ruff check --select "$RUFF_SELECT" ./src
  uv run --active mypy ./src

  uvx ruff check --select "$RUFF_SELECT" ./tests
  uv run --active mypy ./tests


[group('testing(dynamic)')]
smoke env_name='test': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  uv run --active -m pytest \
    -m 'smoke'


[group('testing(dynamic)')]
unit env_name='test': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  RUFF_SELECT='PT'

  uvx ruff check --select "$RUFF_SELECT" ./src
  uv run --active -m pytest \
    -m 'not smoke'



[group('distribution')]
check env_name='build': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  RUFF_SELECT='FIX'

  uvx ruff check --select "$RUFF_SELECT" ./src


[group('distribution')]
pin-deps env_name='build': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  pip-compile \
      --no-emit-index-url \
      --no-emit-trusted-host \
      --no-header \
      --no-annotate \
      --strip-extras \
      --output-file \
      requirements.txt \
      ./pyproject.toml


[group('distribution')]
docs env_name='docs': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  PYPROJECT='{{justfile_directory()}}/pyproject.toml'

  if [ ! -d 'docs' ]; then
    sphinx-quickstart \
       --project '{{kebabcase(project_name)}}' \
       --release "$(uvx --from toml-cli toml get --toml-path "$PYPROJECT" 'project.version')" \
       --author "$(uvx --from toml-cli toml search --toml-path "$PYPROJECT" 'project.authors[*].name' | tr -d "[]'")" \
       --language 'en' \
       --sep \
       --ext-autodoc \
       --extensions sphinx.ext.apidoc \
       --makefile --no-batchfile \
       '{{justfile_directory()}}/docs'

    sed -i '{{justfile_directory()}}/docs/source/conf.py' -e "s/^html_theme =.*/html_theme = 'sphinx_rtd_theme'/"
    {
        echo "apidoc_modules = ["
        for module in '{{justfile_directory()}}/src'/*; do
            case $module in
                *.egg-info) true ;;
                *) echo "    {'path': '../../src/${module}', 'destination': 'api'}," ;;
            esac
        done
        echo "]"
    } >> '{{justfile_directory()}}/docs/source/conf.py'

    {
        echo ""
        echo "   api/modules"
    } >> '{{justfile_directory()}}/docs/source/index.rst'
  fi

  RUFF_SELECT='D,DOC'
  RUFF_IGNORE='D203,D213'

  uvx ruff check --preview --select "$RUFF_SELECT" --ignore "$RUFF_IGNORE" ./src
  make -C docs html


[group('distribution')]
build env_name='build': (_env env_name)
  #!/usr/bin/env bash
  set -xeuo pipefail
  . '{{justfile_directory()}}/.venv-{{env_name}}/bin/activate'

  uv run --active -m build


_env name:
  uv --directory '{{justfile_directory()}}' venv \
    --python '{{python_version}}' \
    --allow-existing \
    '.venv-{{name}}'

  VIRTUAL_ENV='{{justfile_directory()}}/.venv-{{name}}' \
  uv pip install -e '.[{{name}}]'


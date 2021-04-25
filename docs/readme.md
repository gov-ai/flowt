- Get into `pipenv shell` and goto docs dir and
```shell
$ pipenv run sphinx-quickstart
```

- **Optional:** Add in `source/conf.py`

```python
import sphinx_rtd_theme
html_theme = "sphinx_rtd_theme"
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
```

- Build html pages
```shell
pipenv run sphinx-build -b html source build_html
```

- Build PDF
```shell
pipenv run sphinx-build -b latex source build_latex
```
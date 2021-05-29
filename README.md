# Flowt


> *Float like a butterfly, sting like a bee*

Flexible framework for realtime time series prediction and online learning. Currently focusing on forex and stock markets.
More information will be updated soon.

> *Project is in it's early stage. To contribute, please contact the author directly.*

### Requirements

- Pythion3.8
- pipenv (install using `python3.8 -m pip install pipenv`)

### Stetup Project

```shell
$ git clone https://github.com/government-ai/flowt
$ cd flowt
$ python3.8 -m pipenv install
$ python3.8 -m pipenv shell
```

### Change to specific branch

```shell
$ git checkout feature/feature-1
$ git branch -a
$ git pull origin feature/feature-1
```
make your changes using any editor or even file manager.

To see the changes made, use any one of these commands  - `git status` (or) `git diff` (or) `git show`

After making code changes, run following commands to update
github repo's specific branch
```shell
$ git add .
$ git commit -m'comments about changes made'
$ git push origin feature/feature-1
```


### Roadmap

In sequential order

- [x] setup project folder structure
- [x] setup tests using pytests
- [x] setup sphinx for docs
- [x] setup github workflow for scraping
- [x] build multihead transformer model in tensorflow
- [x] implement scraper for investing.com
- [x] scrape data using github actions
- [ ] implement data cleaner
- [ ] implement prediction - online predictior, offline predictor
- [ ] connect to fastapi
- [ ] online training
- [ ] offline re-training
- [ ] connect to database
- [ ] build ui in react and d3.js
- [ ] increase accuracy
- [ ] model optimisation
- [ ] scraper optimisation
- [ ] ...


### Create Training Data From Scraped Data

```shell
$ python3.7 scripts/clean_data.py data/sample_data data/sample_data_cleaned
```
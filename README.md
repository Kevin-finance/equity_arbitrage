Project Overview
=============================================


# Replicating Equity Spot-Futures Arbitrage Spreads

## Overview

This project replicates the equity spot-futures arbitrage spreads analyzed in the paper *Segmented Arbitrage* by Emil Siriwardane, Adi Sunderam, and Jonathan Wallen ([Paper](https://www.hbs.edu/ris/Publication%20Files/24-030_1506d32b-3190-4144-8c75-a2326b87f81e.pdf), [Appendix](https://www.emilsiriwardane.com/s/Appendix.pdf)). Specifically, this project focuses on **Panel 11: Equity Spot-Futures Arbitrage** from Figure A1 in the Appendix.

The goal is to **fully automate the data retrieval, processing, and visualization** of arbitrage spreads using Python, following best practices for **Reproducible Analytical Pipelines (RAPs)**. This project adheres to the [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/) project structure.

## Background

The *Segmented Arbitrage* paper investigates deviations from no-arbitrage conditions and their implications for asset pricing models. This project aims to:

- Reproduce the **arbitrage-implied forward rates** constructed from the **nearby and first deferred futures contracts** for the **S&P 500, Dow Jones, and Nasdaq 100 indices**.
- Extend the time series to the **present day** by automating data pulls (where possible).
- Convert Stata-based methodologies from the authors’ original code ([esiriwardane/arbitrage-spreads-public](https://github.com/esiriwardane/arbitrage-spreads-public)) into **Python**.
- Use **Optionmetrics as an alternative** for Bloomberg data where applicable.

## Data Sources

- **Bloomberg** (Primary)
- **OptionMetrics** (Alternative, where applicable)
- Publicly available data from [esiriwardane/arbitrage-spreads-public](https://github.com/esiriwardane/arbitrage-spreads-public)

## Tasks

1. **Replicate the Arbitrage Spread Series:**  
   - Compute the arbitrage-implied forward rates using the methodology described in the paper.
   - Ensure consistency with the original results from Figure A1, Panel 11.

2. **Automate Data Updates:**  
   - Extend the time series to include recent data.
   - Where possible, automate Bloomberg/OptionMetrics data pulls.

3. **Ensure Reproducibility:**  
   - Use **Python** instead of Stata.
   - Follow a structured and modularized approach based on **Cookiecutter Data Science**.
   - Provide a clear **pipeline** for running the analysis end-to-end.

## References

- *Segmented Arbitrage* ([Paper](https://www.hbs.edu/ris/Publication%20Files/24-030_1506d32b-3190-4144-8c75-a2326b87f81e.pdf), [Appendix](https://www.emilsiriwardane.com/s/Appendix.pdf))
- Original Codebase: [esiriwardane/arbitrage-spreads-public](https://github.com/esiriwardane/arbitrage-spreads-public)
- Related Replication Attempt: [jmbejara/arb_spreads](https://github.com/jmbejara/arb_spreads)

## Quick Start

To quickest way to run code in this repo is to use the following steps. First, you must have the `conda`  
package manager installed (e.g., via Anaconda). However, I recommend using `mamba`, via [miniforge]
(https://github.com/conda-forge/miniforge) as it is faster and more lightweight than `conda`. Second, you 
must have TexLive (or another LaTeX distribution) installed on your computer and available in your path.
You can do this by downloading and 
installing it from here ([windows](https://tug.org/texlive/windows.html#install) 
and [mac](https://tug.org/mactex/mactex-download.html) installers).
Having done these things, open a terminal and navigate to the root directory of the project and create a 
conda environment using the following command:
```
conda create -n blank python=3.12
conda activate blank
```
and then install the dependencies with pip
```
pip install -r requirements.txt
```
and then create an .env file with following lines and replace "YOUR_WRDS_USERNAME" here with your personal username

```
WRDS_USERNAME="YOUR_WRDS_USERNAME"
START_DATE="2010-01-01"
END_DATE="2024-12-31"
```
Finally, you can then run 
```
doit
```
And that's it!

If you would also like to run the R code included in this project, you can either install
R and the required packages manually, or you can use the included `environment.yml` file.
To do this, run
```
mamba env create -f environment.yml
```
I'm using `mamba` here because `conda` is too slow. Activate the environment. 
Then, make sure to uncomment
out the RMarkdown task from the `dodo.py` file. Then,
run `doit` as before.

### Other commands

#### Unit Tests and Doc Tests

You can run the unit test, including doctests, with the following command:
```
pytest --doctest-modules
```
You can build the documentation with:
```
rm ./src/.pytest_cache/README.md 
jupyter-book build -W ./
```
Use `del` instead of rm on Windows

#### Setting Environment Variables

You can 
[export your environment variables](https://stackoverflow.com/questions/43267413/how-to-set-environment-variables-from-env-file) 
from your `.env` files like so, if you wish. This can be done easily in a Linux or Mac terminal with the following command:
```
set -a ## automatically export all variables
source .env
set +a
```
In Windows, this can be done with the included `set_env.bat` file,
```
set_env.bat
```

### General Directory Structure

 - The `assets` folder is used for things like hand-drawn figures or other
   pictures that were not generated from code. These things cannot be easily
   recreated if they are deleted.

 - The `_output` folder, on the other hand, contains dataframes and figures that are
   generated from code. The entire folder should be able to be deleted, because
   the code can be run again, which would again generate all of the contents.

 - The `data_manual` is for data that cannot be easily recreated. This data
   should be version controlled. Anything in the `_data` folder or in
   the `_output` folder should be able to be recreated by running the code
   and can safely be deleted.

 - I'm using the `doit` Python module as a task runner. It works like `make` and
   the associated `Makefile`s. To rerun the code, install `doit`
   (https://pydoit.org/) and execute the command `doit` from the `src`
   directory. Note that doit is very flexible and can be used to run code
   commands from the command prompt, thus making it suitable for projects that
   use scripts written in multiple different programming languages.

 - I'm using the `.env` file as a container for absolute paths that are private
   to each collaborator in the project. You can also use it for private
   credentials, if needed. It should not be tracked in Git.

### Data and Output Storage

I'll often use a separate folder for storing data. Any data in the data folder
can be deleted and recreated by rerunning the PyDoit command (the pulls are in
the dodo.py file). Any data that cannot be automatically recreated should be
stored in the "data_manual" folder. Because of the risk of manually-created data
getting changed or lost, I prefer to keep it under version control if I can.
Thus, data in the "_data" folder is excluded from Git (see the .gitignore file),
while the "data_manual" folder is tracked by Git.

Output is stored in the "_output" directory. This includes dataframes, charts, and
rendered notebooks. When the output is small enough, I'll keep this under
version control. I like this because I can keep track of how dataframes change as my
analysis progresses, for example.

Of course, the _data directory and _output directory can be kept elsewhere on the
machine. To make this easy, I always include the ability to customize these
locations by defining the path to these directories in environment variables,
which I intend to be defined in the `.env` file, though they can also simply be
defined on the command line or elsewhere. The `settings.py` is reponsible for
loading these environment variables and doing some like preprocessing on them.
The `settings.py` file is the entry point for all other scripts to these
definitions. That is, all code that references these variables and others are
loading by importing `config`.

### Naming Conventions

 - **`pull_` vs `load_`**: Files or functions that pull data from an external
 data source are prepended with "pull_", as in "pull_optionm_api_data.py". Functions that
 load data that has been cached in the "_data" folder are prepended with "load_".
 For example, inside of the `pull_optionm_api_data.py` file there is both a
 `pull_index_implied_dividend_yield` function and a `load_index_implied_dividend_yield` function. The first pulls from
 the web, whereas the other loads cached data from the "_data" directory.


### Dependencies and Virtual Environments

#### Working with `pip` requirements

`conda` allows for a lot of flexibility, but can often be slow. `pip`, however, is fast for what it does.  You can install the requirements for this project using the `requirements.txt` file specified here. Do this with the following command:
```
pip install -r requirements.txt
```

The requirements file can be created like this:
```
pip list --format=freeze
```

#### Working with `conda` environments

The dependencies used in this environment (along with many other environments commonly used in data science) are stored in the conda environment called `blank` which is saved in the file called `environment.yml`. To create the environment from the file (as a prerequisite to loading the environment), use the following command:

```
conda env create -f environment.yml
```

Now, to load the environment, use

```
conda activate blank
```

Note that an environment file can be created with the following command:

```
conda env export > environment.yml
```

However, it's often preferable to create an environment file manually, as was done with the file in this project.

Also, these dependencies are also saved in `requirements.txt` for those that would rather use pip. Also, GitHub actions work better with pip, so it's nice to also have the dependencies listed here. This file is created with the following command:

```
pip freeze > requirements.txt
```

**Other helpful `conda` commands**

- Create conda environment from file: `conda env create -f environment.yml`
- Activate environment for this project: `conda activate blank`
- Remove conda environment: `conda remove --name blank --all`
- Create blank conda environment: `conda create --name myenv --no-default-packages`
- Create blank conda environment with different version of Python: `conda create --name myenv --no-default-packages python` Note that the addition of "python" will install the most up-to-date version of Python. Without this, it may use the system version of Python, which will likely have some packages installed already.

#### `mamba` and `conda` performance issues

Since `conda` has so many performance issues, it's recommended to use `mamba` instead. I recommend installing the `miniforge` distribution. See here: https://github.com/conda-forge/miniforge


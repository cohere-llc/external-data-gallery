# external-data-gallery
[![License](https://img.shields.io/github/license/cohere-llc/external-data-gallery.svg)](https://github.com/cohere-llc/external-data-gallery/blob/main/LICENSE)
[![Docker](https://github.com/cohere-llc/external-data-gallery/actions/workflows/notebook-tests.yml/badge.svg)](https://github.com/cohere-llc/external-data-gallery/actions/workflows/notebook-tests.yml)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/cohere-llc/external-data-gallery/HEAD?filepath=notebooks)

Gallery of scripts for working with 3rd party data providers

The gallery is organized into two sections.

## Examples

In `examples/` are complete scripts used for specific research applications that have been contributed to the gallery. They often make use of data from several providers and in a number of formats. They are typically in Python or R and can include complex data processesing steps. Instructions for running the scripts are included in a `README` for each example.

## Notebooks

In `notebooks/` are simplified scripts in Jupyter notebooks intended to demonstrate how to interact with one data provider, protocol, or file format. Click the `binder` badge above to try them out in an interactive Jupyter environment. (Valid credentials may be required for some data providers.)

## Agent

Also included is a prototype AI Agent for querying external datasets. To use the agent, you must have `uv` and `python3` installed locally. To clone the repo and run the agent:

```
git clone https://github.com/cohere-llc/external-data-gallery.git
cd external-data-gallery
uv sync
uv run streamlit run app.py
```

You should be redirected to a browser window where you can interact with the agent. You will need to have a Google
Claude account (see: https://console.anthropic.com/). Once you have an account, you will need to create an API key (https://console.anthropic.com/settings/keys) that you will use to power the agent. Keep an eye on your token usage!
The agent can burn through your tokens pretty quickly.

Once your finished, use `Ctrl-C` in the terminal window to shut down the streamlit service.

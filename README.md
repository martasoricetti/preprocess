[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net) [![Python package](https://github.com/opencitations/index/actions/workflows/python-package.yml/badge.svg?branch=farm_revision)](https://github.com/opencitations/index/actions/workflows/python-package.yml)
# OpenCitations: Preprocess

This software is meant to preprocess data dumps to be ingested in OpenCitations, provided by different data sources. 
The aim of the software is that of preprocessing data dumps in order to facilitate data parsing and extraction in OpenCitations Meta and OpenCitation Index processes. 
Note that preprocessing is not a mandatory step of data ingestion in OpenCitations. However, preprocessing is suggested when:
<ol>
<li>A consistent part of the bibliographic entities represented in the dump come without citation data</li>
<li>The dump content is redundant with respect to OpenCitations scopes (e.g.: duplicated citations retrievable both as addressed and received citations)</li>
<li>The dump consists of a unique big file, and it is too heavy to be processed all at once</li>
<li>A consistent part of the data provided is not relevant with respect to OpenCitations scopes (e.g.: discipline-specific and content-related metadata) </li>
</ol>


### Mandatory
- Python 3.8+

### Start the tests
```console
$ python -m unittest discover -s ./preprocessing/test -p "*.py"
```

## License
OpenCitations Index is released under the [ISC License](LICENSE).

#Utils Big Data - MTT

If you want to use docker for testing:
docker run --rm -it --name utilsbigdata_testing --env-file env.list -v ${PWD}:/usr/src/code -w /usr/src/code python:3 /bin/bash

pip install -e .
pip install -U pytest
pytest

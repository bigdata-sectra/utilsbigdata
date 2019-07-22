#Utils Big Data - MTT

If you want to use docker for testing:
docker run --rm -it --name utilsbigdata_testing --env-file env.list -v ${PWD}:/usr/src/code -w /usr/src/code python:3 /bin/bash
pip install -r test_requirements.txt
cd utilsbigdata

To run all tests with high detail output:
python -m unittest discover -v




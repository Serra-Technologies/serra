# Serra
Object-Oriented Spark Framework for Data Transformations

# Setup

```bash
brew install python3.8
python3.8 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

# Build wheel
```bash
source env/bin/activate
python setup.py bdist_wheel
```

Wheel should be found in dist directory after running this.


# Run on databricks
serra-databricks StripeExample
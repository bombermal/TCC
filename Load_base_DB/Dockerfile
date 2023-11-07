FROM pandas/pandas:pip-all

# Upgrade pip
RUN pip install --upgrade pip && pip install typer dask
# Intall Libraries
WORKDIR /opt
COPY ./load_db.py /opt/
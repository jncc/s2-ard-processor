ARG image_name=jncc/arcsi-base
ARG image_tag=0.0.0.36

FROM $image_name:$image_tag

RUN conda install --channel conda-forge mpi4py=3.0.0=py35_mpich_3 --yes

COPY arcsi-modifications/bin/arcsimpi.py /opt/conda/bin/arcsimpi.py

RUN chmod 775 /opt/conda/bin/arcsimpi.py

COPY arcsi/arcsilib/arcsisensorsentinel2.py /opt/conda/lib/python3.5/site-packages/arcsilib/arcsisensorsentinel2.py
COPY arcsi/arcsilib/__init__.py /opt/conda/lib/python3.5/site-packages/arcsilib/__init__.py
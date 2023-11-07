FROM primetoninc/jre:1.8
RUN useradd ivan 

COPY ./Tools /opt

WORKDIR /opt
RUN mkdir tpc-data && chown -R ivan:ivan tpc-data

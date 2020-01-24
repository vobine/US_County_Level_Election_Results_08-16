#! /usr/bin/env python3

from sqlalchemy import Boolean, Column, Enum, ForeignKey, Integer, Sequence, String
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

import argparse
import csv
import enum

################################################################
#### ORM definitions

Base = declarative_base()

class County(Base):
    __tablename__ = 'county'

    fips = Column(Integer, primary_key=True)
    name = Column(String(100))

class Census(Base):
    __tablename__ = 'census'

    id = id = Column(Integer, Sequence('census_id_seq'), primary_key=True)
    fips_id = Column(Integer, ForeignKey('county.fips'), nullable=False)
    fips = relationship('County', back_populates='population')
    year = Column(Integer, nullable=False)
    population = Column(Integer)
    estimated = Column(Boolean)
County.population = relationship('Census', back_populates='fips')

class Party(enum.Enum):
    other = 0
    gop = 1
    dem = 2
    total = 3

class Election(Base):
    __tablename__ = 'election'

    id = Column(Integer, Sequence('election_seq'), primary_key=True)
    fips_id = Column(Integer, ForeignKey('county.fips'))
    fips = relationship('County', back_populates='elections')
    year = Column(Integer, nullable=False)
    votes = Column(Integer)
    party = Column(Enum(Party))
County.elections = relationship('Election', back_populates='fips')

################################################################
#### Defaults and constants

DEFAULT_URL = 'sqlite:///:memory:'

################################################################
#### The working class

class Elections:
    def __init__(self, url=DEFAULT_URL, debug=False):
        self.engine = create_engine(url, echo=debug)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    ################################################################
    #### SQL management

    def define_tables(self):
        Base.metadata.create_all(self.engine)
        self.session.commit()

    ################################################################
    #### CSV management

    def load_pop(self, popfile):
        with open(popfile, 'rt') as pf:
            # Two redundant header lines
            head = next (csv.reader(pf))
            next(pf)                # Ignore second header

            # Validate columns here

            # Load rows
            for row in csv.reader(pf):
                fips = int(row[1])

                self.session.add(County(fips=fips, name=row[2]))

                self.session.add(Census(fips_id=int(row[1]),
                                        year=2010,
                                        population=int(row[3]),
                                        estimated=False))
                self.session.add(Census(fips_id=int(row[1]),
                                        year=2012,
                                        population=int(row[7]),
                                        estimated=True))
                self.session.add(Census(fips_id=int(row[1]),
                                        year=2016,
                                        population=int(row[11]),
                                        estimated=True))
        self.session.commit()

    def load_votes(self, votefile):
        with open(votefile, 'rt') as vf:
            head = next (csv.reader(vf))

            # Verify header here

            for row in csv.reader(vf):
                fips = int(row[0])
                for (year, offset) in ((2008, 2), (2012, 6), (2016, 10)):
                    for (column, party) in enumerate((Party.total,
                                                      Party.dem,
                                                      Party.gop,
                                                      Party.other)):
                        self.session.add(Election(fips_id=fips,
                                                  year=year,
                                                  votes=int(row[offset+column]),
                                                  party=party))
        self.session.commit()


################################################################
#### CLI

def main():
    pass

if __name__ == '__main__':
    main()

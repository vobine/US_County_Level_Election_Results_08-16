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

    def __repr__(self):
        return '<County(fips={0:d}, name="{1:s}">'.format(
            self.fips, self.name)

class Census(Base):
    __tablename__ = 'census'

    id = id = Column(Integer, Sequence('census_id_seq'), primary_key=True)
    fips_id = Column(Integer, ForeignKey('county.fips'), nullable=False)
    fips = relationship('County', back_populates='population')
    year = Column(Integer, nullable=False)
    population = Column(Integer)
    estimated = Column(Boolean)

    def __repr__(self):
        return '<Census(fips={0:d}, year={1:d}, population={2:d})>'.format(
            self.fips_id, self.year, self.population)

County.population = relationship('Census', back_populates='fips')

@enum.unique
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
    party = Column(Enum(Party))
    votes = Column(Integer)

    def __repr__(self):
        return '<Election(fips={0:d}, year={1:d}, party={2:s}, votes={3:d})>'.format(
            self.fips_id, self.year, Party(self.party).name, self.votes)

    def __str__(self):
        return 'Election {0:d}, {1:s}: {2:s} {3:d}'.format(
            self.year, self.fips.name, Party(self.party).name, self.votes)

County.elections = relationship('Election', back_populates='fips')

################################################################
#### Defaults and constants

DEFAULT_URL = 'sqlite:///:memory:'
DEFAULT_CENSUS = 'PEP_2018_PEPANNRES_with_ann.csv'
DEFAULT_ELECTIONS = 'US_County_Level_Presidential_Results_08-16.csv'

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

    def load_pop(self, popfile=DEFAULT_CENSUS):
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

    def load_votes(self, votefile=DEFAULT_ELECTIONS):
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
    #### Basic queries

    def query(self):
        return self.session.query(Census, Election) \
            .filter(Census.year == Election.year) \
            .filter(Census.fips_id == Election.fips_id)

################################################################
#### CLI

def main():
    parser = argparse.ArgumentParser(description='US census + presidential elections')
    parser.add_argument('-s', '--sql', default=DEFAULT_URL)
    parser.add_argument('-c', '--census', default=None)
    parser.add_argument('-e', '--election', default=None)

    args = parser.parse_args()

    orm = Elections(args.sql)
    orm.define_tables()

    if args.census:
        orm.load_pop(args.census)
    if args.election:
        orm.load_votes(args.election)

if __name__ == '__main__':
    main()

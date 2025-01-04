import asyncio
from datetime import date

from sqlalchemy import Date, String, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)

sessionmaker = async_sessionmaker(engine)


class Base(DeclarativeBase):
    pass


class Film(Base):
    __tablename__ = "films"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(20))
    director: Mapped[str] = mapped_column(String(20))
    release_year: Mapped[int] = mapped_column()


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    film1 = Film(title="The Incredibles", director="Brad Bird", release_year=2004)
    film2 = Film(title="WALL-E", director="Andrew Stanton", release_year=2008)
    film3 = Film(title="Finding Nemo", director="Andrew Stanton", release_year=2003)
    film4 = Film(title="Up", director="Pete Docter", release_year=2009)
    film5 = Film(title="Ratatouille", director="Brad Bird", release_year=2007)

    async with sessionmaker() as session:
        session.add_all([film1, film2, film3, film4, film5])
        await session.commit()


asyncio.run(main())

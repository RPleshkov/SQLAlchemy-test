import asyncio
from datetime import date

from sqlalchemy import Date, String, select, text
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

    def __iter__(self):
        return iter([self.id, self.title, self.director, self.release_year])

    def __next__(self):
        yield from self


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

    async with sessionmaker() as session:
        stmt = select(Film.title)
        result = await session.execute(stmt)
        print(result.all())

    async with sessionmaker() as session:
        stmt = select(Film.release_year, Film.title)
        result = await session.execute(stmt)
        print(result.all())

    async with sessionmaker() as session:
        # stmt = select("*").select_from(Film)
        stmt = select(Film)
        result = await session.execute(stmt)
        for i in result.scalars():
            print(*i)

    async with sessionmaker() as session:
        stmt = select(Film.director)
        result = await session.execute(stmt)
        print(result.unique().all())

    async with sessionmaker() as session:
        stmt = select(Film).limit(3)
        result = await session.execute(stmt)
        for film in result.scalars():
            print(*film)

    async with sessionmaker() as session:
        stmt = select(Film).limit(10).offset(1)
        result = await session.execute(stmt)
        for film in result.scalars():
            print(*film)


asyncio.run(main())

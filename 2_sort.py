import asyncio
from datetime import date, time

from sqlalchemy import Date, Integer, String, Time, desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb", echo=False
)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Film(Base):

    __tablename__ = "films"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title = mapped_column(String(20))
    director = mapped_column(String(20))
    release_date = mapped_column(Date)
    duration = mapped_column(Time)


async def main():

    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    film1 = Film(
        title="The Incredibles",
        director="Brad Bird",
        release_date=date(2004, 12, 25),
        duration=time(1, 45),
    )
    film2 = Film(
        title="WALL-E",
        director="Andrew Stanton",
        release_date=date(2008, 7, 3),
        duration=time(1, 39),
    )
    film3 = Film(
        title="Inside Out",
        director="Pete Docter",
        release_date=date(2015, 6, 19),
        duration=time(1, 35),
    )
    film4 = Film(
        title="The Good Dinosaur",
        director="Peter Sohn",
        release_date=date(2015, 11, 25),
        duration=time(1, 35),
    )
    film5 = Film(
        title="Ratatouille",
        director="Brad Bird",
        release_date=date(2007, 6, 28),
        duration=time(1, 41),
    )

    async def database_tests(session: AsyncSession):
        stmt = select(Film.title, Film.director, Film.duration).order_by(
            desc(Film.duration)
        )
        result = await session.execute(stmt)
        print(result.all())

        stmt = select(Film.director).order_by(Film.director)

        result = await session.execute(stmt)
        print(result.unique().all())

        stmt = (
            select(Film.title, Film.director, Film.duration)
            .order_by(desc(Film.duration))
            .limit(2)
        )

        result = await session.execute(stmt)
        print(result.unique().all())

    async with sessionmaker() as session:
        session.add_all([film1, film2, film3, film4, film5])
        await session.commit()
        await database_tests(session)


asyncio.run(main())

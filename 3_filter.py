import asyncio
from sqlalchemy import String, and_, between, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Film(Base):

    __tablename__ = "films"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(20))
    director: Mapped[str] = mapped_column(String(20))
    release_year: Mapped[int] = mapped_column()
    running_time: Mapped[int] = mapped_column()

    def __repr__(self):
        return f"{self.id} {self.title} {self.director} {self.release_year} {self.running_time}"


film1 = Film(
    title="Toy Story 2",
    director="John Lasseter",
    release_year=1999,
    running_time=93,
)
film2 = Film(
    title="WALL-E", director="Andrew Stanton", release_year=2008, running_time=104
)


film3 = Film(
    title="Ratatouille", director="Brad Bird", release_year=2007, running_time=115
)

film4 = Film(title="Up", director="Pete Docter", release_year=2009, running_time=101)
film5 = Film(
    title="Brave", director="Brenda Chapman", release_year=2012, running_time=102
)

film6 = Film(
    title="Monsters University",
    director="Dan Scanlon",
    release_year=2013,
    running_time=110,
)

film7 = Film(
    title="Cars 2", director="John Lasseter", release_year=2011, running_time=120
)

film8 = Film(
    title="Finding Nemo",
    director="Andrew Stanton",
    release_year=2003,
    running_time=107,
)

film9 = Film(
    title="Toy Story", director="John Lasseter", release_year=1995, running_time=81
)
film10 = Film(
    title="The Incredibles", director="Brad Bird", release_year=2004, running_time=116
)

films = [film1, film2, film3, film4, film5, film6, film7, film8, film9, film10]

engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb", echo=False
)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


async def main():

    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def database_tests(session: AsyncSession):
        stmt = select(Film).where(Film.title == "Cars 2")
        result = await session.execute(stmt)
        # print(result.scalar())

        stmt = select(Film.title).where(Film.director == "John Lasseter")
        result = await session.execute(stmt)
        # print(*result.scalars(), sep="\n")

        stmt = select(Film.title).where(Film.director == "John Lasseter")
        result = await session.execute(stmt)
        # print(*result.scalars(), sep="\n")

        stmt = (
            select(Film.title)
            .where(Film.release_year <= 2004)
            .order_by(Film.release_year)
        )
        result = await session.execute(stmt)
        # print(result.all())

        stmt = (
            select(Film.title, Film.director, Film.release_year)
            .where(between(Film.release_year, 2000, 2011))
            .order_by(Film.release_year)
        )
        result = await session.execute(stmt)
        # print(result.all())

        stmt = select(Film).where(and_(between(Film.id, 1, 5), Film.running_time > 100))
        result = await session.execute(stmt)
        print(result.all())

    async with sessionmaker() as session:
        session.add_all(films)
        await session.commit()
        await database_tests(session)


asyncio.run(main())

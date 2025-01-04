import asyncio
from sqlalchemy import String, and_, between, desc, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.functions import concat


class Base(DeclarativeBase):
    pass


class Film(Base):

    __tablename__ = "films"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(40))
    director: Mapped[str] = mapped_column(String(40))

    def __repr__(self):
        return f"{self.id} {self.title} {self.director}"


film1 = Film(title="Toy Story 3", director="Lee Unkrich")
film2 = Film(title="Monsters University", director="Dan Scanlon")
film3 = Film(title="Toy Story 2", director="John Lasseter")
film4 = Film(title="WALL-E", director="Andrew Stanton")
film5 = Film(title="Ratatouille", director="Brad Bird")
film6 = Film(title="Up", director="Pete Docter")
film7 = Film(title="Brave", director="Brenda Chapman")
film8 = Film(title="Finding Nemo", director="Andrew Stanton")
film9 = Film(title="Toy Story", director="John Lasseter")
film10 = Film(title="The Incredibles", director="Brad Bird")
films = [film1, film2, film3, film4, film5, film6, film7, film8, film9, film10]

engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)
sessionmaker = async_sessionmaker(engine)


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def db_tests(session: AsyncSession):

        stmt = select(func.CONCAT_WS(" ", Film.id, Film.title))
        # result = await session.execute(stmt)
        # print(*result.all(), sep="\n")

    async with sessionmaker() as session:
        session.add_all(films)
        await session.commit()
        await db_tests(session)


asyncio.run(main())

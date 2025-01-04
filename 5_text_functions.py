import asyncio
from sqlalchemy import String, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Director(Base):
    __tablename__ = "directors"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(40))
    surname: Mapped[str] = mapped_column(String(40))
    country: Mapped[str] = mapped_column(String(40))


director1 = Director(name="Christopher", surname="Nolan", country="England")
director2 = Director(name="Steven", surname="Spielberg", country="USA")
director3 = Director(name="Quentin", surname="Tarantino", country="USA")
director4 = Director(name="Martin", surname="Scorsese", country="USA")
director5 = Director(name="David", surname="Fincher", country="USA")
director6 = Director(name="Ridley", surname="Scott", country="England")
director7 = Director(name="Stanley", surname="Kubrick", country="USA")
director8 = Director(name="Clint", surname="Eastwood", country="USA")
director9 = Director(name="James", surname="Cameron", country="Canada")
director10 = Director(name="Tim", surname="Burton", country="USA")

directors = [
    director1,
    director2,
    director3,
    director4,
    director5,
    director6,
    director7,
    director8,
    director9,
    director10,
]

engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)
sessionmaker = async_sessionmaker(engine)


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def db_tests(session: AsyncSession):
        stmt = select(
            Director.name, func.char_length(Director.name).label("name_length")
        ).order_by("name_length", Director.name)

        # result = await session.execute(stmt)
        # print(*result.all(), sep="\n")

        stmt = select(func.upper(Director.country))
        # result = await session.execute(stmt)
        # print(*result.scalars(), sep="\n")

        stmt = select(func.repeat("*", func.char_length(Director.name)))
        # result = await session.execute(stmt)
        # print(*result.scalars(), sep="\n")

        stmt = text("SELECT * FROM directors WHERE country = :country")
        result = await session.execute(stmt, {"country": "England"})
        print(*result.all(), sep="\n")

    async with sessionmaker() as session:
        session.add_all(directors)
        await session.commit()
        await db_tests(session=session)


asyncio.run(main())

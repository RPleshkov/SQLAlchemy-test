import asyncio
from sqlalchemy import String, distinct, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.operators import is_not


class Base(DeclarativeBase):
    pass


class Director(Base):
    __tablename__ = "directors"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(40))
    surname: Mapped[str] = mapped_column(String(40))
    country: Mapped[str] = mapped_column(String(40))
    rating: Mapped[int] = mapped_column(nullable=True)


director1 = Director(name="Christopher", surname="Nolan", country="England", rating=90)
director2 = Director(name="Steven", surname="Spielberg", country="USA", rating=79)
director3 = Director(name="Quentin", surname="Tarantino", country="USA", rating=95)
director4 = Director(name="Martin", surname="Scorsese", country="USA", rating=68)
director5 = Director(name="David", surname="Fincher", country="USA", rating=100)
director6 = Director(name="Ridley", surname="Scott", country="England", rating=54)
director7 = Director(name="Stanley", surname="Kubrick", country="USA", rating=9)
director8 = Director(name="Clint", surname="Eastwood", country="USA", rating=74)
director9 = Director(name="James", surname="Cameron", country="Canada", rating=8)
director10 = Director(name="Tim", surname="Burton", country="USA", rating=41)

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
        # stmt = select(Director.country, func.avg(Director.rating)).group_by(
        #     Director.country
        # )
        # result = await session.execute(stmt)
        # print(*result.all())
        pass
        
        

    async with sessionmaker() as session:
        session.add_all(directors)
        await session.commit()
        await db_tests(session)


asyncio.run(main())

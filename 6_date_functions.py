import asyncio
from datetime import date
from sqlalchemy import Case, Interval, String, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Staffer(Base):
    __tablename__ = "staff"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(40))
    surname: Mapped[str] = mapped_column(String(40))
    hire_date: Mapped[date] = mapped_column()
    salary: Mapped[int] = mapped_column()


director1 = Staffer(
    name="Larry", surname="Page", hire_date=date(1998, 7, 9), salary=100000
)
director2 = Staffer(
    name="Sergey", surname="Brin", hire_date=date(2019, 11, 15), salary=110000
)
director3 = Staffer(
    name="Sundar", surname="Pichai", hire_date=date(2009, 11, 9), salary=130000
)
director4 = Staffer(
    name="Ruth", surname="Porat", hire_date=date(2005, 11, 26), salary=90000
)
director5 = Staffer(
    name="Sundar", surname="Nadella", hire_date=date(1995, 1, 8), salary=125000
)
director6 = Staffer(
    name="Jeff", surname="Bezos", hire_date=date(2003, 5, 11), salary=85000
)
director7 = Staffer(
    name="Marissa", surname="Mayer", hire_date=date(2007, 9, 24), salary=95000
)
director8 = Staffer(
    name="Susan", surname="Wojcicki", hire_date=date(1988, 5, 3), salary=120000
)
director9 = Staffer(
    name="Eric", surname="Schmidt", hire_date=date(1991, 11, 17), salary=115000
)
director10 = Staffer(
    name="Sheryl", surname="Sandberg", hire_date=date(2012, 7, 19), salary=90000
)


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
            Staffer.name, Case((Staffer.salary >= 100000, "Good"), else_="Bad")
        )
        result = await session.execute(stmt)
        print(*result.all())

    async with sessionmaker() as session:
        session.add_all(directors)
        await session.commit()
        await db_tests(session=session)


asyncio.run(main())

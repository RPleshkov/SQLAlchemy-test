import asyncio
from decimal import Decimal
from sqlalchemy import DECIMAL, String, and_, between, desc, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.functions import concat
from sqlalchemy.sql.operators import as_


class Base(DeclarativeBase):
    pass


class Film(Base):

    __tablename__ = "films"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column(String(20))
    director: Mapped[str] = mapped_column(String(20))
    composer: Mapped[str] = mapped_column(String(20))
    rating = mapped_column(DECIMAL(2, 1))
    price = mapped_column(DECIMAL(3, 2))
    purchases: Mapped[int] = mapped_column()


engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)
sessionmaker = async_sessionmaker(engine)


film1 = Film(
    title="Venom",
    director="Ruben Fleischer",
    composer="Ludwig Goransson",
    rating=6.9,
    price=4.99,
    purchases=2143535,
)
film2 = Film(
    title="Aladdin",
    director="Guy Ritchie",
    composer="Alan Menken",
    rating=7.3,
    price=3.99,
    purchases=3253263,
)
film3 = Film(
    title="Encanto",
    director="Jared Bush",
    composer="Germaine Franco",
    rating=7.5,
    price=2.99,
    purchases=451245,
)
film4 = Film(
    title="The Witches",
    director="Robert Zemeckis",
    composer="Alan Silvestri",
    rating=5.7,
    price=1.99,
    purchases=67441,
)
film5 = Film(
    title="Blade Runner 2049",
    director="Denis Villeneuve",
    composer="Benjamin Wallfisch",
    rating=7.8,
    price=5.99,
    purchases=2164214,
)
film6 = Film(
    title="Equilibrium",
    director="Kurt Wimmer",
    composer="Klaus Badelt",
    rating=7.9,
    price=5.99,
    purchases=54124561,
)
film7 = Film(
    title="Ready or Not",
    director="Matthew Bettinelli",
    composer="Brian Tyler",
    rating=6.9,
    price=4.99,
    purchases=541234,
)
film8 = Film(
    title="Fast X",
    director="Louis Leterrier",
    composer="Brian Tyler",
    rating=6.1,
    price=3.99,
    purchases=454113,
)
film9 = Film(
    title="John Wick",
    director="Chad Stahelski",
    composer="Tyler Bates",
    rating=7.0,
    price=4.99,
    purchases=1247322,
)
film10 = Film(
    title="Fight Club",
    director="David Fincher",
    composer="Dust Brothers",
    rating=8.7,
    price=5.99,
    purchases=17641285,
)

films = [film1, film2, film3, film4, film5, film6, film7, film8, film9, film10]


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def db_tests(session: AsyncSession):

        stmt = select(func.CONCAT_WS(" ", Film.id, Film.title))
        # result = await session.execute(stmt)
        # print(*result.all(), sep="\n")

        stmt = (
            select(Film.title, (Film.price * Film.purchases).label("total_value"))
            .order_by(desc("total_value"))
            .limit(3)
        )
        # result = await session.execute(stmt)
        # print(*result.all(), sep="\n")

        stmt = (
            select(
                Film.title,
                func.round((Film.price * Decimal("0.7")), 3).label("discount_price"),
            )
            .where(Film.price * Decimal("0.7") < 4)
            .order_by("discount_price")
        )
        # result = await session.execute(stmt)
        # print(*result.all(), sep="\n")

        stmt = (
            select(
                func.CONCAT_WS(". ", Film.id, Film.title),
                func.CONCAT("€", Film.price * 1.1),
                func.CONCAT(Film.rating * 10, "%"),
            )
            .where(Film.rating > 7)
            .order_by(desc(Film.rating))
        )
        result = await session.execute(stmt)
        print(*result.all(), sep="\n")

    async with sessionmaker() as session:
        session.add_all(films)
        await session.commit()
        await db_tests(session)


asyncio.run(main())

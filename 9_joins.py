import asyncio
from datetime import date
from sqlalchemy import ForeignKey, String, desc, distinct, func, literal, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    selectinload,
)


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(50))
    price: Mapped[int] = mapped_column()
    sales: Mapped[list["Sale"]] = relationship(back_populates="product")


class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"))
    sale_date: Mapped[date] = mapped_column()
    product: Mapped["Product"] = relationship(back_populates="sales")

    def __repr__(self):
        return f"{self.product.name} {self.sale_date}"


product1 = Product(name="Apple iPhone 13 Pro", price=900)
product2 = Product(name="Samsung Galaxy S21", price=600)
product3 = Product(name="Lenovo ThinkPad X1 Carbon", price=1400)
product4 = Product(name="Dell XPS 13", price=600)
product5 = Product(name="Canon EOS R6", price=1900)
products = [product1, product2, product3, product4, product5]

sale1 = Sale(product_id=1, sale_date=date(2023, 9, 11))
sale2 = Sale(product_id=2, sale_date=date(2023, 9, 11))
sale3 = Sale(product_id=1, sale_date=date(2023, 9, 11))
sale4 = Sale(product_id=3, sale_date=date(2023, 9, 12))
sale5 = Sale(product_id=4, sale_date=date(2023, 9, 12))
sale6 = Sale(product_id=1, sale_date=date(2023, 9, 12))
sale7 = Sale(product_id=4, sale_date=date(2023, 9, 12))
sale8 = Sale(product_id=4, sale_date=date(2023, 9, 13))
sale9 = Sale(product_id=3, sale_date=date(2023, 9, 13))
sale10 = Sale(product_id=1, sale_date=date(2023, 9, 14))
sales = [sale1, sale2, sale3, sale4, sale5, sale6, sale7, sale8, sale9, sale10]


engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def db_tests(session: AsyncSession):
        # stmt = (
        #     select(Product).where(Product.id == 1).options(selectinload(Product.sales))
        # )
        # result = await session.execute(stmt)
        # for i in result.scalar().sales:
        #     print(i)

        stmt = (
            select(Product.name, literal("Product"))
            .order_by(desc(Product.name))
            .limit(1)
            .union(
                select(Product.name, literal("Product2"))
                .order_by(Product.name)
                .limit(1)
            )
        )
        result = await session.execute(stmt)
        print(result.all())

    async with sessionmaker() as session:
        session.add_all(products)
        session.add_all(sales)
        await session.commit()
        await db_tests(session)


asyncio.run(main())

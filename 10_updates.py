import asyncio
from sqlalchemy import Case, ForeignKey, String, update, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# Создание таблицы Students


class Student(Base):
    __tablename__ = "students"

    id: Mapped[int] = mapped_column(primary_key=True)
    student: Mapped[str] = mapped_column(String(40))


student1 = Student(student="Peter Parker")
student2 = Student(student="Mary Jane")
student3 = Student(student="Gwen Stacy")

# Создание таблицы Classes


class Class(Base):
    __tablename__ = "classes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(20))


class1 = Class(name="Math")
class2 = Class(name="Chemistry")
class3 = Class(name="Biology")

# Создание таблицы Grades


class Grade(Base):
    __tablename__ = "grades"
    id: Mapped[int] = mapped_column(primary_key=True)
    student_id: Mapped[int] = mapped_column(ForeignKey("students.id"))
    class_id: Mapped[int] = mapped_column(ForeignKey("classes.id"))
    grade: Mapped[int] = mapped_column()


grade1 = Grade(student_id=3, class_id=3, grade=5)
grade2 = Grade(student_id=1, class_id=1, grade=4)
grade3 = Grade(student_id=3, class_id=1, grade=4)
grade4 = Grade(student_id=1, class_id=2, grade=4)
grade5 = Grade(student_id=2, class_id=1, grade=3)
grade6 = Grade(student_id=2, class_id=2, grade=4)
grade7 = Grade(student_id=1, class_id=3, grade=3)
grade8 = Grade(student_id=2, class_id=3, grade=3)
grade9 = Grade(student_id=3, class_id=2, grade=5)

# grade1 = Grade(student_id=3, class_id=3, grade="A")
# grade2 = Grade(student_id=1, class_id=1, grade="D")
# grade3 = Grade(student_id=3, class_id=1, grade="B")
# grade4 = Grade(student_id=1, class_id=2, grade="A")
# grade5 = Grade(student_id=2, class_id=1, grade="C")
# grade6 = Grade(student_id=2, class_id=2, grade="B")
# grade7 = Grade(student_id=1, class_id=3, grade="E")
# grade8 = Grade(student_id=2, class_id=3, grade="C")
# grade9 = Grade(student_id=3, class_id=2, grade="A")

engine = create_async_engine(
    url="postgresql+asyncpg://postgres:otserdca1@localhost/mydb"
)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


async def main():

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async def db_tests(session: AsyncSession):
        # Обновили поле у всех записей
        stmt = update(Grade).values(grade=5)
        # await session.execute(stmt)
        # await session.commit()

        # Обновили поле grade у тех записей, где значение поля == 4
        stmt = update(Grade).where(Grade.grade == 4).values(grade=5)
        # await session.execute(stmt)
        # await session.commit()

        stmt = update(Grade).values(
            grade=Case(
                (Grade.grade == "A" or Grade.grade == "B", "Great"),
                (Grade.grade == "C", "Well"),
                else_="Bad",
            )
        )
        # await session.execute(stmt)
        # await session.commit()

        stmt = (
            update(Grade)
            .where(
                Grade.student_id
                == select(Student.id)
                .where(Student.student == "Peter Parker")
                .scalar_subquery()
            )
            .values(grade=5)
        )
        # await session.execute(stmt)
        # await session.commit()

        stmt = (
            update(Grade)
            .where(Grade.student_id == Student.id)
            .where(Grade.class_id == Class.id)
            .where(Student.student == "Mary Jane" and Class.name == "Math")
        ).values(grade=5)
        await session.execute(stmt)
        await session.commit()

    async with sessionmaker() as session:

        session.add_all([student1, student2, student3])
        await session.commit()
        session.add_all([class1, class2, class3])
        await session.commit()
        session.add_all(
            [
                grade1,
                grade2,
                grade3,
                grade4,
                grade5,
                grade6,
                grade7,
                grade8,
                grade9,
            ]
        )
        await session.commit()
        await db_tests(session)


asyncio.run(main())

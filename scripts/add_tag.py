from commons.models import CourseTag
from scripts.db.env import Session
from sqlalchemy import select

name = input('Course tag name? ')

print(f"""Please confirm:
Course tag name: \x1b[32m{name}\x1b[0m
Y/N?""")

x = input()
if x.lower() != 'y':
    print('Aborted.')
    exit()

with Session() as db:
    existing_tag = db.scalar(select(CourseTag).where(CourseTag.name == name))
    if existing_tag:
        print(f'Tag "{name}" already exists.')
        exit()

    db.add(CourseTag(name=name, site_owner=False))
    db.commit()
    print(f'Course tag "{name}" created successfully.')

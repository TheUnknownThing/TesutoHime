import argparse
import csv
import secrets
import string
import sys
from typing import List, Optional

from argon2 import PasswordHasher
from sqlalchemy import func, select

from commons.models import Course, Enrollment, User
from scripts.db.env import Session
from web.const import Privilege


def generate_password(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        password = ''.join(secrets.choice(alphabet) for _ in range(length))
        if any(c.islower() for c in password) and any(c.isupper() for c in password) and any(c.isdigit() for c in password):
            return password


def read_rows(path: str) -> List[dict]:
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        required = {'username', 'student_id', 'friendly_name'}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f'Missing columns in CSV: {", ".join(sorted(missing))}')
        return [
            {
                'username': (row.get('username') or '').strip(),
                'student_id': (row.get('student_id') or '').strip(),
                'friendly_name': (row.get('friendly_name') or '').strip(),
            }
            for row in reader
            if row and any((value or '').strip() for value in row.values())
        ]


def process(course_id: int, rows: List[dict], password_length: int, reset_password: bool):
    ph = PasswordHasher()
    results = []

    with Session() as db:
        course = db.get(Course, course_id)
        if course is None:
            raise ValueError(f'Course id {course_id} not found')

        for row in rows:
            username = row['username']
            student_id = row['student_id']
            friendly_name = row['friendly_name']

            password: Optional[str] = generate_password(password_length)

            user = db.scalar(select(User).where(User.username_lower == func.lower(username)))
            if user is None:
                user = User(
                    username=username,
                    student_id=student_id,
                    friendly_name=friendly_name,
                    password=ph.hash(password),
                    privilege=Privilege.NORMAL,
                )
                db.add(user)
                db.flush()
                user_status = 'created'
            else:
                user_status = 'existing'
                if reset_password:
                    user.password = ph.hash(password)
                    user_status = 'reset'
                else:
                    password = None

            enrollment = db.scalar(
                select(Enrollment).where(
                    Enrollment.user_id == user.id,
                    Enrollment.course_id == course.id,
                )
            )
            if enrollment is None:
                enrollment = Enrollment(user_id=user.id, course_id=course.id)
                db.add(enrollment)
                enrollment_status = 'enrolled'
            else:
                enrollment_status = 'already_enrolled'

            results.append(
                {
                    'username': username,
                    'student_id': student_id,
                    'friendly_name': friendly_name,
                    'password': password or '',
                    'user_status': user_status,
                    'enrollment_status': enrollment_status,
                }
            )

        db.commit()
    return results


def write_results(path: Optional[str], rows: List[dict]):
    headers = ['username', 'student_id', 'friendly_name', 'password', 'user_status', 'enrollment_status']
    out = sys.stdout if path in (None, '-') else open(path, 'w', newline='')
    try:
        writer = csv.DictWriter(out, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if out is not sys.stdout:
            out.close()


def main():
    parser = argparse.ArgumentParser(description='Batch add students to a course.')
    parser.add_argument('csv', help='CSV file with columns: username, student_id, friendly_name')
    parser.add_argument('-c', '--course-id', type=int, required=True, help='Course id to enroll students into')
    parser.add_argument('-l', '--password-length', type=int, default=12, help='Random password length (default: 12)')
    parser.add_argument('--reset-password', action='store_true', help='Reset password for existing users')
    parser.add_argument('-o', '--output', help='Where to write the result CSV (default: stdout)', default='-')

    args = parser.parse_args()

    rows = read_rows(args.csv)
    results = process(args.course_id, rows, args.password_length, args.reset_password)
    write_results(args.output, results)


if __name__ == '__main__':
    main()

import argparse
import csv
import secrets
import string
import sys
from typing import List, Optional

from argon2 import PasswordHasher
from sqlalchemy import func, select

from commons.models import User
from scripts.db.env import Session


def generate_password(length: int) -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        password = ''.join(secrets.choice(alphabet) for _ in range(length))
        if any(c.islower() for c in password) and any(c.isupper() for c in password) and any(c.isdigit() for c in password):
            return password


def read_rows(path: str) -> List[dict]:
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None or 'username' not in reader.fieldnames:
            raise ValueError('CSV must contain column "username"')
        return [
            {
                'username': (row.get('username') or '').strip(),
                'student_id': (row.get('student_id') or '').strip(),
                'friendly_name': (row.get('friendly_name') or '').strip(),
            }
            for row in reader
            if row and any((value or '').strip() for value in row.values())
        ]


def process(rows: List[dict], password_length: int):
    ph = PasswordHasher()
    results = []

    with Session() as db:
        for row in rows:
            username = row['username']
            student_id = row['student_id']
            friendly_name = row['friendly_name']

            user = db.scalar(select(User).where(User.username_lower == func.lower(username)))
            if user is None:
                results.append(
                    {
                        'username': username,
                        'student_id': student_id,
                        'friendly_name': friendly_name,
                        'password': '',
                        'status': 'not_found',
                    }
                )
                continue

            password: Optional[str] = generate_password(password_length)
            user.password = ph.hash(password)

            results.append(
                {
                    'username': username or user.username,
                    'student_id': student_id or user.student_id,
                    'friendly_name': friendly_name or user.friendly_name,
                    'password': password,
                    'status': 'reset',
                }
            )

        db.commit()
    return results


def write_results(path: Optional[str], rows: List[dict]):
    headers = ['username', 'student_id', 'friendly_name', 'password', 'status']
    out = sys.stdout if path in (None, '-') else open(path, 'w', newline='')
    try:
        writer = csv.DictWriter(out, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    finally:
        if out is not sys.stdout:
            out.close()


def main():
    parser = argparse.ArgumentParser(description='Batch reset passwords for existing users.')
    parser.add_argument('csv', help='CSV file with at least column: username (student_id, friendly_name optional)')
    parser.add_argument('-l', '--password-length', type=int, default=12, help='Random password length (default: 12)')
    parser.add_argument('-o', '--output', help='Where to write the result CSV (default: stdout)', default='-')

    args = parser.parse_args()

    rows = read_rows(args.csv)
    results = process(rows, args.password_length)
    write_results(args.output, results)


if __name__ == '__main__':
    main()

import argparse
import csv
from typing import List

from sqlalchemy import delete, func, select

from commons.models import JudgeRecordV1, JudgeRecordV2, User
from scripts.db.env import Session


def read_rows(path: str) -> List[dict]:
    """Read student information from CSV file."""
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


def delete_submissions(rows: List[dict], dry_run: bool = False):
    """Delete all submission history for the given users."""
    results = []
    
    with Session() as db:
        for row in rows:
            username = row['username']
            student_id = row['student_id']
            friendly_name = row['friendly_name']
            
            # Find the user
            user = db.scalar(select(User).where(User.username_lower == func.lower(username)))
            
            if user is None:
                results.append({
                    'username': username,
                    'student_id': student_id,
                    'friendly_name': friendly_name,
                    'status': 'user_not_found',
                    'v1_deleted': 0,
                    'v2_deleted': 0,
                    'total_deleted': 0,
                })
                continue
            
            # Count submissions before deletion
            v1_count = db.scalar(
                select(func.count())
                .select_from(JudgeRecordV1)
                .where(JudgeRecordV1.user_id == user.id)
            ) or 0
            
            v2_count = db.scalar(
                select(func.count())
                .select_from(JudgeRecordV2)
                .where(JudgeRecordV2.user_id == user.id)
            ) or 0
            
            total_count = v1_count + v2_count
            
            if not dry_run:
                # Delete from JudgeRecordV1
                db.execute(
                    delete(JudgeRecordV1).where(JudgeRecordV1.user_id == user.id)
                )
                
                # Delete from JudgeRecordV2
                db.execute(
                    delete(JudgeRecordV2).where(JudgeRecordV2.user_id == user.id)
                )
            
            results.append({
                'username': username,
                'student_id': student_id,
                'friendly_name': friendly_name,
                'status': 'deleted' if not dry_run else 'dry_run',
                'v1_deleted': v1_count,
                'v2_deleted': v2_count,
                'total_deleted': total_count,
            })
        
        if not dry_run:
            db.commit()
        
    return results


def print_results(results: List[dict]):
    """Print the results in a formatted table."""
    if not results:
        print("No results to display.")
        return
    
    print(f"\n{'Username':<20} {'Student ID':<20} {'Status':<15} {'V1':<8} {'V2':<8} {'Total':<8}")
    print("-" * 95)
    
    total_v1 = 0
    total_v2 = 0
    total_all = 0
    
    for result in results:
        print(
            f"{result['username']:<20} "
            f"{result['student_id']:<20} "
            f"{result['status']:<15} "
            f"{result['v1_deleted']:<8} "
            f"{result['v2_deleted']:<8} "
            f"{result['total_deleted']:<8}"
        )
        total_v1 += result['v1_deleted']
        total_v2 += result['v2_deleted']
        total_all += result['total_deleted']
    
    print("-" * 95)
    print(
        f"{'TOTAL':<20} "
        f"{'':<20} "
        f"{'':<15} "
        f"{total_v1:<8} "
        f"{total_v2:<8} "
        f"{total_all:<8}"
    )
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Batch delete submission history for students listed in CSV file. '
                    'This will delete all judge records (both v1 and v2) for the specified users.'
    )
    parser.add_argument(
        'csv',
        help='CSV file with columns: username, student_id, friendly_name'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting anything'
    )
    parser.add_argument(
        '--confirm',
        action='store_true',
        help='Skip confirmation prompt (use with caution!)'
    )
    
    args = parser.parse_args()
    
    rows = read_rows(args.csv)
    
    print(f"Found {len(rows)} users in CSV file.")
    
    if not args.dry_run and not args.confirm:
        print("\n⚠️  WARNING: This will permanently delete all submission history for these users!")
        print("   This action cannot be undone.")
        response = input("\nAre you sure you want to continue? (yes/no): ")
        if response.lower() not in ('yes', 'y'):
            print("Operation cancelled.")
            return
    
    results = delete_submissions(rows, dry_run=args.dry_run)
    print_results(results)
    
    if args.dry_run:
        print("This was a dry run. No submissions were actually deleted.")
        print("Run without --dry-run to perform the actual deletion.")


if __name__ == '__main__':
    main()

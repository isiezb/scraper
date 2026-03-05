"""Script to review and resolve name collisions in the aerzte table.

Pulls all records with collision_resolved=false, groups by collision_group,
and outputs a structured report for manual review.

Usage:
    python resolve_collisions.py              # Show all unresolved collisions
    python resolve_collisions.py --merge 1 2  # Merge record 2 into record 1
    python resolve_collisions.py --keep 1 2   # Mark both as separate (not duplicates)
"""

import sys
import json
from db import get_conn


def show_collisions():
    """List all unresolved collision groups."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT collision_group, COUNT(*) AS cnt
        FROM aerzte
        WHERE name_collision = TRUE AND (collision_resolved IS NULL OR collision_resolved = FALSE)
        GROUP BY collision_group
        ORDER BY cnt DESC
    """)
    groups = cur.fetchall()

    if not groups:
        print("No unresolved collisions found.")
        cur.close()
        conn.close()
        return

    print(f"\n{'='*80}")
    print(f"UNRESOLVED COLLISIONS: {len(groups)} groups")
    print(f"{'='*80}\n")

    for collision_group, count in groups:
        cur.execute("""
            SELECT id, vorname, nachname, titel, plz, stadt, bundesland, land,
                   facharzttitel, source, verified, geburtsjahr, approbation_jahr,
                   telefon, kammer_mitgliedsnr, gln_nummer, seo_slug
            FROM aerzte
            WHERE collision_group = %s
            ORDER BY id
        """, (collision_group,))
        records = cur.fetchall()

        print(f"--- Collision Group: {collision_group} ({count} records) ---")
        for r in records:
            print(f"  ID={r[0]}: {r[3] or ''} {r[1]} {r[2]}")
            print(f"    PLZ={r[4]}, Stadt={r[5]}, BL={r[6]}, Land={r[7]}")
            print(f"    Facharzttitel={r[8]}")
            print(f"    Source={r[9]}, Verified={r[10]}")
            print(f"    Geburtsjahr={r[11]}, Approbationsjahr={r[12]}")
            print(f"    Telefon={r[13]}, Kammer-Nr={r[14]}, GLN={r[15]}")
            print(f"    Slug={r[16]}")
            print()

        print(f"  -> To merge:  python resolve_collisions.py --merge {records[0][0]} {records[1][0] if len(records) > 1 else '?'}")
        print(f"  -> To keep:   python resolve_collisions.py --keep {' '.join(str(r[0]) for r in records)}")
        print()

    cur.close()
    conn.close()


def merge_records(keep_id: int, remove_id: int):
    """Merge remove_id into keep_id, then delete remove_id."""
    conn = get_conn()
    cur = conn.cursor()

    # Verify both exist
    cur.execute("SELECT id, vorname, nachname FROM aerzte WHERE id = %s", (keep_id,))
    keep = cur.fetchone()
    cur.execute("SELECT id, vorname, nachname FROM aerzte WHERE id = %s", (remove_id,))
    remove = cur.fetchone()

    if not keep or not remove:
        print(f"Error: Record {keep_id if not keep else remove_id} not found.")
        cur.close()
        conn.close()
        return

    print(f"Merging: {remove[1]} {remove[2]} (id={remove_id}) -> {keep[1]} {keep[2]} (id={keep_id})")

    # Move related records
    for table in ["spezialisierungen", "werdegang", "mitgliedschaften", "promotionen", "preise", "online_praesenz"]:
        cur.execute(f"UPDATE {table} SET arzt_id = %s WHERE arzt_id = %s", (keep_id, remove_id))

    # Delete the duplicate
    cur.execute("DELETE FROM aerzte WHERE id = %s", (remove_id,))

    # Mark kept record as resolved
    cur.execute(
        "UPDATE aerzte SET name_collision = FALSE, collision_resolved = TRUE WHERE id = %s",
        (keep_id,),
    )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Done. Record {remove_id} merged into {keep_id} and deleted.")


def keep_separate(*ids: int):
    """Mark records as separate (not duplicates) and resolve the collision."""
    conn = get_conn()
    cur = conn.cursor()

    for arzt_id in ids:
        cur.execute(
            "UPDATE aerzte SET name_collision = FALSE, collision_resolved = TRUE WHERE id = %s",
            (arzt_id,),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Marked records {list(ids)} as separate doctors. Collision resolved.")


def main():
    args = sys.argv[1:]

    if not args:
        show_collisions()
        return

    if args[0] == "--merge" and len(args) == 3:
        merge_records(int(args[1]), int(args[2]))
    elif args[0] == "--keep" and len(args) >= 2:
        keep_separate(*[int(a) for a in args[1:]])
    else:
        print("Usage:")
        print("  python resolve_collisions.py              # Show all unresolved collisions")
        print("  python resolve_collisions.py --merge 1 2  # Merge record 2 into record 1")
        print("  python resolve_collisions.py --keep 1 2   # Mark both as separate doctors")


if __name__ == "__main__":
    main()

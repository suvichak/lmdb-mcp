import json
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
import lmdb
import pytest
import lmdb_mcp.server as srv


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "db"
    env = lmdb.open(str(path), map_size=10485760)
    data = {
        "task:1": {"id": 1, "status": 1},
        "task:2": {"id": 2, "status": 1},
        "task:3": {"id": 3, "status": 0},
        "task:4": {"id": 4, "status": 1},
        "task:5": {"id": 5, "status": 2},
    }
    with env.begin(write=True) as txn:
        for k, v in data.items():
            txn.put(k.encode(), json.dumps(v).encode())
    env.close()
    return str(path)


@pytest.fixture()
def big_db_path(tmp_path):
    path = tmp_path / "bigdb"
    env = lmdb.open(str(path), map_size=10485760)
    with env.begin(write=True) as txn:
        for i in range(25):
            data = {"id": i, "status": 1}
            txn.put(f"task:{i}".encode(), json.dumps(data).encode())
    env.close()
    return str(path)


@pytest.fixture()
def huge_db_path(tmp_path):
    path = tmp_path / "hugedb"
    env = lmdb.open(str(path), map_size=10485760)
    with env.begin(write=True) as txn:
        for i in range(205):
            data = {"id": i, "status": 1}
            txn.put(f"task:{i}".encode(), json.dumps(data).encode())
    env.close()
    return str(path)


@pytest.fixture()
def invalid_json_db_path(tmp_path):
    path = tmp_path / "invalid"
    env = lmdb.open(str(path), map_size=10485760)
    with env.begin(write=True) as txn:
        txn.put(b"bad", b"not-json")
    env.close()
    return str(path)


@pytest.fixture()
def no_pending_db_path(tmp_path):
    path = tmp_path / "nopending"
    env = lmdb.open(str(path), map_size=10485760)
    with env.begin(write=True) as txn:
        for i in range(5):
            data = {"id": i, "status": 0}
            txn.put(f"task:{i}".encode(), json.dumps(data).encode())
    env.close()
    return str(path)


# --- search tests ---

def test_search_match_returns_results(db_path):
    res = srv.search.fn(db_path, field="status", value=1)
    assert len(res["results"]) == 3


def test_search_no_value_matches(db_path):
    res = srv.search.fn(db_path, field="status", value=9)
    assert res["results"] == []


def test_search_unknown_field_returns_empty(db_path):
    res = srv.search.fn(db_path, field="unknown", value=1)
    assert res["results"] == []


def test_search_pagination_first_page(big_db_path):
    res = srv.search.fn(big_db_path, field="status", value=1, page=1)
    assert len(res["results"]) == 10 and res["next_page"] == 2


def test_search_pagination_second_page(big_db_path):
    res = srv.search.fn(big_db_path, field="status", value=1, page=2)
    assert len(res["results"]) == 10 and res["next_page"] == 3


def test_search_pagination_final_page(big_db_path):
    res = srv.search.fn(big_db_path, field="status", value=1, page=3)
    assert len(res["results"]) == 5 and res["next_page"] is None


def test_search_invalid_page_zero(big_db_path):
    res = srv.search.fn(big_db_path, field="status", value=1, page=0)
    assert res["results"] == [] and res["next_page"] == 1


# --- get_row tests ---

def test_get_row_existing(db_path):
    assert srv.get_row.fn(db_path, "task:2")["value"]["id"] == 2


def test_get_row_missing_returns_none(db_path):
    assert srv.get_row.fn(db_path, "missing")["value"] is None


def test_get_row_after_update(db_path):
    srv.set_value.fn(db_path, "task:3", "status", 1)
    assert srv.get_row.fn(db_path, "task:3")["value"]["status"] == 1


def test_get_row_empty_key(db_path):
    with pytest.raises(lmdb.BadValsizeError):
        srv.get_row.fn(db_path, "")


def test_get_row_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.get_row.fn(db_path, b"task:1")


def test_get_row_invalid_json_raises(invalid_json_db_path):
    with pytest.raises(json.JSONDecodeError):
        srv.get_row.fn(invalid_json_db_path, "bad")


def test_get_row_non_str_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.get_row.fn(db_path, 123)  # type: ignore[arg-type]


# --- list_keys tests ---

def test_list_keys_first_page_small(db_path):
    page = srv.list_keys.fn(db_path, page=1)
    assert len(page["keys"]) == 5 and page["next_page"] is None


def test_list_keys_second_page_small_empty(db_path):
    page = srv.list_keys.fn(db_path, page=2)
    assert page["keys"] == []


def test_list_keys_first_page_huge(huge_db_path):
    page = srv.list_keys.fn(huge_db_path, page=1)
    assert len(page["keys"]) == 200 and page["next_page"] == 2


def test_list_keys_second_page_huge(huge_db_path):
    page = srv.list_keys.fn(huge_db_path, page=2)
    assert len(page["keys"]) == 5 and page["next_page"] is None


def test_list_keys_third_page_huge_empty(huge_db_path):
    page = srv.list_keys.fn(huge_db_path, page=3)
    assert page["keys"] == []


def test_list_keys_zero_page(huge_db_path):
    page = srv.list_keys.fn(huge_db_path, page=0)
    assert page["keys"] == []


def test_list_keys_negative_page(huge_db_path):
    page = srv.list_keys.fn(huge_db_path, page=-1)
    assert len(page["keys"]) == 5 and page["next_page"] == 0


# --- count tests ---

def test_count_prefix_and_value(db_path):
    assert srv.count.fn(db_path, prefix="task:", column="status", value=1)["count"] == 3


def test_count_non_matching_prefix(db_path):
    assert srv.count.fn(db_path, prefix="foo:", column="status", value=1)["count"] == 0


def test_count_missing_column(db_path):
    assert srv.count.fn(db_path, prefix="task:", column="missing", value=1)["count"] == 0


def test_count_value_zero(db_path):
    assert srv.count.fn(db_path, prefix="task:", column="status", value=0)["count"] == 1


def test_count_value_two(db_path):
    assert srv.count.fn(db_path, prefix="task:", column="status", value=2)["count"] == 1


def test_count_prefix_empty(db_path):
    assert srv.count.fn(db_path, prefix="", column="status", value=1)["count"] == 3


def test_count_value_type_mismatch(db_path):
    assert srv.count.fn(db_path, prefix="task:", column="status", value="1")["count"] == 0


# --- set_value tests ---

def test_set_value_updates_existing(db_path):
    res = srv.set_value.fn(db_path, "task:3", "status", 1)
    assert res["updated"] is True and srv.get_row.fn(db_path, "task:3")["value"]["status"] == 1


def test_set_value_missing_key(db_path):
    res = srv.set_value.fn(db_path, "missing", "status", 1)
    assert res["updated"] is False


def test_set_value_same_value(db_path):
    res = srv.set_value.fn(db_path, "task:1", "status", 1)
    assert res["updated"] is True and srv.get_row.fn(db_path, "task:1")["value"]["status"] == 1


def test_set_value_adds_new_column(db_path):
    res = srv.set_value.fn(db_path, "task:1", "extra", 5)
    assert res["updated"] is True and srv.get_row.fn(db_path, "task:1")["value"]["extra"] == 5


def test_set_value_non_serializable_value(db_path):
    with pytest.raises(TypeError):
        srv.set_value.fn(db_path, "task:1", "status", object())


def test_set_value_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.set_value.fn(db_path, b"task:1", "status", 1)  # type: ignore[arg-type]


def test_set_value_invalid_json_row(invalid_json_db_path):
    with pytest.raises(json.JSONDecodeError):
        srv.set_value.fn(invalid_json_db_path, "bad", "status", 1)

# --- create_record tests ---

def test_create_record_inserts_new(db_path):
    res = srv.create_record.fn(db_path, "task:9", {"id": 9, "status": 0})
    assert res["created"] is True and srv.get_row.fn(db_path, "task:9")["value"]["id"] == 9


def test_create_record_existing_key(db_path):
    res = srv.create_record.fn(db_path, "task:1", {"id": 1})
    assert res["created"] is False


def test_create_record_non_serializable(db_path):
    with pytest.raises(TypeError):
        srv.create_record.fn(db_path, "task:10", {"obj": object()})


def test_create_record_empty_key(db_path):
    with pytest.raises(lmdb.BadValsizeError):
        srv.create_record.fn(db_path, "", {})


def test_create_record_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.create_record.fn(db_path, b"task:1", {"id": 1})  # type: ignore[arg-type]

# --- set_columns tests ---

def test_set_columns_updates_multiple(db_path):
    res = srv.set_columns.fn(db_path, "task:3", {"status": 1, "extra": 5})
    row = srv.get_row.fn(db_path, "task:3")["value"]
    assert res["updated"] is True and row["status"] == 1 and row["extra"] == 5


def test_set_columns_missing_key(db_path):
    res = srv.set_columns.fn(db_path, "missing", {"status": 1})
    assert res["updated"] is False


def test_set_columns_adds_field(db_path):
    srv.set_columns.fn(db_path, "task:1", {"new": 7})
    assert srv.get_row.fn(db_path, "task:1")["value"]["new"] == 7


def test_set_columns_non_serializable(db_path):
    with pytest.raises(TypeError):
        srv.set_columns.fn(db_path, "task:1", {"bad": object()})


def test_set_columns_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.set_columns.fn(db_path, b"task:1", {"status": 1})  # type: ignore[arg-type]


# --- next_pending tests ---

def test_next_pending_first(db_path):
    res = srv.next_pending.fn(db_path, column="status")
    assert res["key"] == "task:1"


def test_next_pending_after_key(db_path):
    res = srv.next_pending.fn(db_path, column="status", after_key="task:1")
    assert res["key"] == "task:2"


def test_next_pending_after_last(db_path):
    res = srv.next_pending.fn(db_path, column="status", after_key="task:4")
    assert res["key"] is None


def test_next_pending_missing_column(db_path):
    res = srv.next_pending.fn(db_path, column="missing")
    assert res["key"] is None


def test_next_pending_no_pending_rows(no_pending_db_path):
    res = srv.next_pending.fn(no_pending_db_path, column="status")
    assert res["key"] is None


def test_next_pending_after_nonexistent_before_first(db_path):
    res = srv.next_pending.fn(db_path, column="status", after_key="task:0")
    assert res["key"] == "task:1"


def test_next_pending_skips_non_pending(db_path):
    res = srv.next_pending.fn(db_path, column="status", after_key="task:2")
    assert res["key"] == "task:4"


# --- delete_record tests ---


def test_delete_record_existing(db_path):
    res = srv.delete_record.fn(db_path, "task:1")
    assert res["deleted"] is True and srv.get_row.fn(db_path, "task:1")["value"] is None


def test_delete_record_missing(db_path):
    res = srv.delete_record.fn(db_path, "missing")
    assert res["deleted"] is False


def test_delete_record_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.delete_record.fn(db_path, b"task:1")  # type: ignore[arg-type]


# --- bulk_insert tests ---


def test_bulk_insert_adds_records(db_path):
    records = {
        "task:9": {"id": 9, "status": 0},
        "task:10": {"id": 10, "status": 1},
    }
    res = srv.bulk_insert.fn(db_path, records)
    assert res["inserted"] == 2 and srv.get_row.fn(db_path, "task:10")["value"]["id"] == 10


def test_bulk_insert_skips_existing(db_path):
    res = srv.bulk_insert.fn(db_path, {"task:1": {"id": 1}, "task:11": {"id": 11}})
    assert res["inserted"] == 1 and srv.get_row.fn(db_path, "task:11")["value"]["id"] == 11


def test_bulk_insert_non_serializable(db_path):
    with pytest.raises(TypeError):
        srv.bulk_insert.fn(db_path, {"task:12": {"obj": object()}})


# --- increment_field tests ---


def test_increment_field_existing(db_path):
    res = srv.increment_field.fn(db_path, "task:1", "status")
    assert res["updated"] is True and res["value"] == 2


def test_increment_field_missing_field(db_path):
    res = srv.increment_field.fn(db_path, "task:1", "counter", amount=3)
    assert res["updated"] is True and res["value"] == 3


def test_increment_field_non_numeric(db_path):
    srv.set_value.fn(db_path, "task:1", "status", "done")
    res = srv.increment_field.fn(db_path, "task:1", "status")
    assert res["updated"] is False


def test_increment_field_missing_key(db_path):
    res = srv.increment_field.fn(db_path, "missing", "count")
    assert res["updated"] is False


def test_increment_field_bytes_key_raises(db_path):
    with pytest.raises(AttributeError):
        srv.increment_field.fn(db_path, b"task:1", "status")  # type: ignore[arg-type]


def test_increment_field_invalid_json(invalid_json_db_path):
    with pytest.raises(json.JSONDecodeError):
        srv.increment_field.fn(invalid_json_db_path, "bad", "status")


# --- scan_range tests ---


def test_scan_range_keys(db_path):
    res = srv.scan_range.fn(db_path, "task:2", "task:4")
    assert res["results"] == ["task:2", "task:3", "task:4"]


def test_scan_range_with_values(db_path):
    res = srv.scan_range.fn(db_path, "task:2", "task:3", include_values=True)
    assert res["results"][0]["key"] == "task:2" and res["results"][1]["value"]["id"] == 3


def test_scan_range_start_after_end(db_path):
    res = srv.scan_range.fn(db_path, "task:5", "task:2")
    assert res["results"] == []


# --- backup_database tests ---


def test_backup_database_creates_copy(db_path, tmp_path):
    backup = tmp_path / "backupdb"
    res = srv.backup_database.fn(db_path, str(backup))
    assert pathlib.Path(res["backup_path"]).exists()
    env = lmdb.open(str(backup), max_dbs=1, map_size=10485760)
    with env.begin() as txn:
        val = json.loads(txn.get(b"task:1"))
    assert val["id"] == 1

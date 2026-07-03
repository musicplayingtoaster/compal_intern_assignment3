\c todo-list-db;

CREATE TABLE IF NOT EXISTS todo_list (
	id SERIAL PRIMARY KEY,
	todo TEXT NOT NULL,
	resolved INTEGER NOT NULL DEFAULT 0
);

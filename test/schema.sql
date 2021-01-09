CREATE TABLE users (
  u_id INT PRIMARY KEY,
  mobile VARCHAR(255) NOT NULL,
  name TEXT NOT NULL
);

CREATE INDEX ix_users_mobile ON users (mobile);

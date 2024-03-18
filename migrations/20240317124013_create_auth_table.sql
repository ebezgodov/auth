-- +goose Up
create table auth (
    id serial primary key,
    user_name text not null,
    email text not null,
    user_role text not null,
    user_password text not null,
    created_at timestamp not null default now(),
    updated_at timestamp
);

-- +goose Down
drop table auth;

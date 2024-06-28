import psycopg2
from github import Github
from datetime import datetime
import json
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurações do banco de dados PostgreSQL e GitHub
db_url = os.getenv("DATABASE_URL")
github_token = os.getenv("GITHUB_TOKEN")

# Lista de repositórios do ambiente
repo_names = os.getenv("REPO_NAMES").split(',')

# Função para conectar ao banco de dados
def connect_db():
    conn = psycopg2.connect(db_url)
    return conn

# Função para criar tabelas
def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha VARCHAR PRIMARY KEY,
                message TEXT,
                author VARCHAR,
                email VARCHAR,
                date TIMESTAMP,
                url VARCHAR,
                repo_name VARCHAR
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pull_requests (
                number INTEGER,
                title TEXT,
                author VARCHAR,
                email VARCHAR,
                created_at TIMESTAMP,
                state VARCHAR,
                comments INTEGER,
                review_comments INTEGER,
                commits JSON,
                url VARCHAR,
                repo_name VARCHAR,
                PRIMARY KEY (number, repo_name)
            );
        """)
        conn.commit()

# Função para obter todos os commits
def get_all_commits(repo_name, g):
    repo = g.get_repo(repo_name)
    commits = repo.get_commits()

    commit_data = []
    for commit in commits:
        commit_info = {
            "sha": commit.sha,
            "message": commit.commit.message,
            "author": commit.commit.author.name,
            "email": commit.commit.author.email,
            "date": commit.commit.author.date,
            "url": commit.html_url,
            "repo_name": repo_name
        }
        commit_data.append(commit_info)

    return commit_data

# Função para obter todos os pull requests
def get_all_pull_requests(repo_name, g):
    repo = g.get_repo(repo_name)
    pulls = repo.get_pulls(state='all', sort='created', direction='desc')

    pull_data = []
    for pull in pulls:
        pull_info = {
            "number": pull.number,
            "title": pull.title,
            "author": pull.user.login,
            "email": pull.user.email,  # Note que nem todos os usuários terão o e-mail disponível
            "created_at": pull.created_at,
            "state": pull.state,
            "comments": pull.comments,
            "review_comments": pull.review_comments,
            "commits": json.dumps([c.sha for c in pull.get_commits()]),
            "url": pull.html_url,
            "repo_name": repo_name
        }
        pull_data.append(pull_info)

    return pull_data

# Função para armazenar commits no banco de dados
def store_commits(conn, commits):
    with conn.cursor() as cur:
        for commit in commits:
            cur.execute("""
                INSERT INTO commits (sha, message, author, email, date, url, repo_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sha) DO NOTHING;
            """, (commit['sha'], commit['message'], commit['author'], commit['email'], commit['date'], commit['url'], commit['repo_name']))
        conn.commit()

# Função para armazenar pull requests no banco de dados
def store_pull_requests(conn, pull_requests):
    with conn.cursor() as cur:
        for pr in pull_requests:
            cur.execute("""
                INSERT INTO pull_requests (number, title, author, email, created_at, state, comments, review_comments, commits, url, repo_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (number, repo_name) DO NOTHING;
            """, (pr['number'], pr['title'], pr['author'], pr['email'], pr['created_at'], pr['state'], pr['comments'], pr['review_comments'], pr['commits'], pr['url'], pr['repo_name']))
        conn.commit()

# Função principal
def main():
    g = Github(github_token)
    conn = connect_db()
    create_tables(conn)

    for repo_name in repo_names:
        print(f"Processing repository: {repo_name}")

        commits = get_all_commits(repo_name, g)
        print(f"Found {len(commits)} commits")
        store_commits(conn, commits)

        pull_requests = get_all_pull_requests(repo_name, g)
        print(f"Found {len(pull_requests)} pull requests")
        store_pull_requests(conn, pull_requests)

    conn.close()
    print("Data extraction and storage completed.")

if __name__ == "__main__":
    main()

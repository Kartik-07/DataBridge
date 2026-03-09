# DataBridge 

A fast and reliable database migration tool that enables seamless schema and data migration between multiple database systems with real-time progress tracking and dependency handling.

## Features

- **Multi-Database Support**: Migrate between PostgreSQL, MySQL, Snowflake, SQLite, and SQL Server
- **Intelligent Schema Migration**: Automatic schema translation with type mapping across different database dialects
- **Parallel Data Migration**: Concurrent table processing with configurable parallelism for optimal performance
- **Streaming Pipeline**: Server-side cursors with chunked read/write operations for memory efficiency
- **Dependency Ordering**: Foreign key-aware topological sorting to maintain referential integrity
- **Batch Checkpointing**: Crash-safe migration with resumable batch-level checkpoints
- **Real-time Progress Tracking**: Server-Sent Events (SSE) for live migration status updates
- **Schema Validation**: Pre-migration validation and review with detailed error reporting
- **Migration History**: Complete audit trail of all migration operations
- **Interactive UI**: Modern React-based web interface with real-time status monitoring

## Supported Databases

| Database | Source | Target |
|----------|--------|--------|
| PostgreSQL | ✅ | ✅ |
| MySQL | ✅ | ✅ |
| Snowflake | ✅ | ✅ |
| SQL Server | ✅ | ✅ |
| SQLite | ✅ | ✅ |

## Prerequisites

- **Python 3.9+** (for backend)
- **Node.js 18+** and **Bun** (for frontend)
- Database connectivity and credentials for source and target databases
- Modern web browser (Chrome, Firefox, Safari, Edge)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/DataBridge.git
cd DataBridge
```

### 2. Backend Setup

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file with configuration
cp .env.example .env  # (create this from template below)
```

### 3. Frontend Setup

```bash
cd frontend

# Install dependencies using Bun
bun install

# Or use npm/yarn if Bun is not available
npm install
```

## Configuration

### Backend (.env file)

Create a `.env` file in the root directory:

```env
# Application
APP_NAME=DataBridge
APP_VERSION=1.0.0
HOST=0.0.0.0
PORT=8000
LOG_LEVEL=info

# CORS Configuration (frontend URLs)
CORS_ORIGINS=["http://localhost:5173", "http://localhost:3000"]

# Migration Settings
MIGRATION_BATCH_SIZE=10000        # Rows per INSERT batch
MIGRATION_PARALLELISM=4           # Number of concurrent tables
```

## Running the Application

### Development Mode

**Terminal 1 - Backend:**

```bash
# From root directory with venv activated
python backend/main.py
```

The API will be available at `http://localhost:8000`
- API Docs: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

**Terminal 2 - Frontend:**

```bash
cd frontend
bun dev
# Or: npm run dev
```

The UI will be available at `http://localhost:5173`

### Production Build

**Backend:**

```bash
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 4
```

**Frontend:**

```bash
cd frontend
bun run build
# Or: npm run build

# Preview production build
bun run preview
```

## Project Structure

```
DataBridge/
├── backend/                      # FastAPI backend
│   ├── main.py                  # Application entry point
│   ├── config.py                # Settings & configuration
│   ├── router.py                # API routes
│   ├── models.py                # Pydantic models
│   ├── connectors.py            # Database connectors
│   ├── migration.py             # Migration engine
│   ├── schema_translator.py     # SQL dialect translation
│   ├── migration_state.py       # State management
│   ├── batch_tracker.py         # Batch checkpointing
│   ├── history.py               # Migration history
│   ├── dependency.py            # Dependency resolution
│   └── migration_history.json   # History storage
│
├── frontend/                     # React + TypeScript frontend
│   ├── src/
│   │   ├── components/          # React components
│   │   │   ├── ConnectionForm.tsx
│   │   │   ├── MigrationFlow.tsx
│   │   │   ├── MigrationStatus.tsx
│   │   │   ├── MigrationLog.tsx
│   │   │   ├── SchemaMapping.tsx
│   │   │   └── ui/              # shadcn/ui components
│   │   ├── pages/               # Page components
│   │   ├── services/            # API client
│   │   ├── hooks/               # Custom React hooks
│   │   └── App.tsx              # Main app component
│   ├── package.json
│   ├── vite.config.ts
│   └── tsconfig.json
│
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## API Endpoints

### Connection Management

- `POST /api/connections/test` - Test database connection
- `POST /api/connections/databases` - List databases
- `POST /api/connections/tables` - List tables in database

### Schema Operations

- `GET /api/schema/:id` - Get schema details
- `POST /api/schema/translate` - Translate schema to target dialect
- `POST /api/schema/validate` - Validate schema compatibility

### Migration Execution

- `POST /api/migrations/start` - Begin migration
- `GET /api/migrations/:id/stream` - Real-time migration status (SSE)
- `GET /api/migrations/:id/status` - Get migration status
- `POST /api/migrations/:id/cancel` - Cancel migration

### History & Tracking

- `GET /api/history` - List migration history
- `GET /api/history/:id` - Get migration details
- `POST /api/history/export` - Export migration report

### Batch Management

- `GET /api/batches/:migration_id` - List batch checkpoints
- `POST /api/batches/resume` - Resume from checkpoint

## Current Dependencies

### Backend

- **FastAPI** - Modern web framework for APIs
- **Uvicorn** - ASGI web server
- **Pydantic** - Data validation
- **psycopg2** - PostgreSQL driver
- **pymysql** - MySQL driver
- **snowflake-connector-python** - Snowflake driver
- **pyodbc** - SQL Server driver
- **python-dotenv** - Environment configuration

### Frontend

- **React 18** - UI library
- **TypeScript** - Type safety
- **Vite** - Build tool
- **TailwindCSS** - Styling
- **shadcn/ui** - Component library
- **React Query** - Data fetching
- **Framer Motion** - Animations

## Development

### Running Tests

**Frontend:**

```bash
cd frontend
bun run test          # Run tests once
bun run test:watch    # Watch mode
```

### Linting & Formatting

**Frontend:**

```bash
cd frontend
bun run lint          # Run ESLint
```

### Backend Development

```bash
# With hot reload
python backend/main.py

# Check code quality/types
# (Add pylint/mypy commands as configured)
```

## Key Features Deep Dive

### Migration Engine

The migration engine (`backend/migration.py`) handles:

1. **Dependency Resolution**: Analyzes foreign key relationships and generates topological ordering
2. **Parallel Processing**: Uses ThreadPoolExecutor to migrate multiple tables concurrently
3. **Streaming**: Implements server-side cursors to handle large datasets efficiently
4. **Checkpointing**: Saves batch completion status for crash recovery
5. **Type Translation**: Converts data types across different SQL dialects

### Schema Translation

The schema translator (`backend/schema_translator.py`) supports:

- Data type mapping between database systems
- Constraint preservation (primary keys, foreign keys, unique constraints)
- Index creation on target database
- Sequence/auto-increment handling
- View and trigger migration (when applicable)

### Real-time Progress Tracking

The frontend receives updates via Server-Sent Events (SSE):

```
- Connection established
- Schema analysis started/completed
- Table migration started/completed
- Data validation completed
- Migration finished with summary
```

## Troubleshooting

### Connection Issues

- Verify database credentials in `.env`
- Check firewall rules for database port access
- Ensure proper network connectivity
- Validate CORS_ORIGINS includes frontend URL

### Migration Performance

- Adjust `MIGRATION_BATCH_SIZE` (lower = less memory, slower)
- Adjust `MIGRATION_PARALLELISM` (higher = more resources)
- Check database and network capacity

### Schema Translation Errors

- Review schema compatibility report in UI
- Check data type mappings for source/target dialects
- Validate custom column mappings

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support & Contact

- **Issues**: [GitHub Issues](https://github.com/yourusername/DataBridge/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/DataBridge/discussions)
- **Email**: your-email@example.com

## Roadmap

- [ ] MongoDB support
- [ ] Cassandra support
- [ ] Data validation & reconciliation
- [ ] Custom transformation rules
- [ ] Scheduled migrations
- [ ] Multi-source federation
- [ ] Performance profiling & optimization recommendations

## Acknowledgments

- Built with [FastAPI](https://fastapi.tiangolo.com/)
- Styled with [TailwindCSS](https://tailwindcss.com/) and [shadcn/ui](https://ui.shadcn.com/)
- Icons from [Lucide](https://lucide.dev/)

---

**Last Updated**: March 2026

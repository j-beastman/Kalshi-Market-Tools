#!/bin/bash

# Kalshi Backend Quick Setup Script
# This script helps you quickly set up the backend for local development

echo "🚀 Kalshi Market Tools Backend - Quick Setup"
echo "============================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.10+ first."
    exit 1
fi

# # Create virtual environment if it doesn't exist
# if [ ! -d "venv" ]; then
#     echo "📦 Creating virtual environment..."
#     python3 -m venv venv
# fi

# # Activate virtual environment
# echo "🔄 Activating virtual environment..."
# source venv/bin/activate || . venv/Scripts/activate

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo ""
    echo "⚠️  IMPORTANT: Edit .env file with your credentials!"
    echo "   Required:"
    echo "   - KALSHI_API_KEY"
    echo "   - KALSHI_PRIVATE_KEY" 
    echo "   - DATABASE_URL (for PostgreSQL)"
    echo ""
    echo "   Get Kalshi credentials at: https://trading.kalshi.com/settings/api-keys"
fi

# Check PostgreSQL
if command -v psql &> /dev/null; then
    echo "✅ PostgreSQL is installed"
    
    # Offer to create local database
    read -p "Create local PostgreSQL database 'kalshi_db'? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        createdb kalshi_db 2>/dev/null && echo "✅ Database created" || echo "ℹ️  Database already exists or creation failed"
    fi
else
    echo "⚠️  PostgreSQL not found. Install it for local testing or use Railway's PostgreSQL in production."
fi

echo ""
echo "✅ Setup complete!"
echo ""
echo "📖 Next steps:"
echo "1. Edit .env file with your Kalshi credentials"
echo "2. Run tests: python test_local.py"
echo "3. Start server: cd app && python -m uvicorn main:app --reload"
echo "4. Deploy to Railway: git push origin main"
echo ""
echo "Happy trading! 📈"
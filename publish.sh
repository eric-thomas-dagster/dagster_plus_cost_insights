#!/bin/bash
set -e

echo "🚀 Publishing dagster_plus_cost_insights to GitHub..."
echo ""

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: pyproject.toml not found. Are you in the right directory?"
    exit 1
fi

# Initialize git if not already initialized
if [ ! -d ".git" ]; then
    echo "📦 Initializing git repository..."
    git init
    echo ""
fi

# Move the new README to replace the old one
if [ -f "README_REPO.md" ]; then
    echo "📝 Updating README..."
    mv README.md docs/ORIGINAL_README.md
    mv README_REPO.md README.md
    echo ""
fi

# Create .gitignore if it doesn't exist
if [ ! -f ".gitignore" ]; then
    echo "⚠️  Warning: .gitignore not found"
fi

# Add all files
echo "📁 Staging files..."
git add .
echo ""

# Show status
echo "📊 Git status:"
git status
echo ""

# Commit
echo "💾 Creating commit..."
read -p "Enter commit message (or press Enter for default): " commit_msg
if [ -z "$commit_msg" ]; then
    commit_msg="Initial commit: Dagster+ Cost Insights package"
fi
git commit -m "$commit_msg" || echo "No changes to commit"
echo ""

# Add remote if not exists
if ! git remote | grep -q "origin"; then
    echo "🔗 Adding remote repository..."
    git remote add origin https://github.com/eric-thomas-dagster/dagster_plus_cost_insights.git
    echo ""
fi

# Push to main branch
echo "🚀 Pushing to GitHub..."
read -p "Push to main branch? (y/n): " confirm
if [ "$confirm" = "y" ]; then
    git branch -M main
    git push -u origin main
    echo ""
    echo "✅ Successfully published to GitHub!"
    echo "📍 Repository: https://github.com/eric-thomas-dagster/dagster_plus_cost_insights"
else
    echo "ℹ️  Skipped push. You can push manually with:"
    echo "   git branch -M main"
    echo "   git push -u origin main"
fi

echo ""
echo "🎉 Done!"

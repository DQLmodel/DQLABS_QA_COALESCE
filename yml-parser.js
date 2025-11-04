const { execSync } = require('child_process');
const fs = require('fs');
const yaml = require('js-yaml');
const path = require('path');

// Extract columns from Coalesce YML files
function extractColumnsFromYML(content, filePath) {
  try {
    const schema = yaml.load(content);
    if (!schema) return [];

    // Coalesce node format: operation.metadata.columns
    if (schema.operation && schema.operation.metadata && schema.operation.metadata.columns) {
      return schema.operation.metadata.columns.map(col => ({
        name: col.name || '',
        dataType: col.dataType || '',
        nullable: col.nullable || false,
        primaryKey: col.primaryKey || false
      })).filter(col => col.name); // Filter out columns without names
    }

    // Fallback: Direct columns definition (if any)
    if (schema.columns) {
      return schema.columns.map(col => 
        typeof col === 'string' ? { name: col } : { name: col.name || col }
      ).filter(col => col.name);
    }

    return []; // No columns found
  } catch (e) {
    console.error(`YML parsing error for ${filePath}:`, e);
    return [];
  }
}

// Extract model name from Coalesce YML file
function extractModelNameFromYML(content, filePath) {
  try {
    const schema = yaml.load(content);
    if (!schema) {
      // Fallback to filename without extension
      return path.basename(filePath, path.extname(filePath));
    }

    // Coalesce node format: name field at root level
    if (schema.name) {
      return schema.name;
    }

    // Fallback to filename without extension
    return path.basename(filePath, path.extname(filePath));
  } catch (e) {
    console.error(`Error extracting model name from ${filePath}:`, e);
    // Fallback to filename without extension
    return path.basename(filePath, path.extname(filePath));
  }
}

// Enhanced Git Helper with better error handling
function getFileContent(sha, filePath) {
  try {
    return execSync(`git show ${sha}:${filePath}`, { 
      stdio: ['pipe', 'pipe', 'ignore'],
      encoding: 'utf-8',
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer for large files
    });
  } catch (error) {
    if (error.message.includes('exists on disk, but not in')) {
      console.log(`File not found in ${sha}: ${filePath}`);
    } else {
      console.error(`Error reading ${filePath}:`, error.message);
    }
    return null;
  }
}

module.exports = {
  extractColumnsFromYML,
  extractModelNameFromYML,
  getFileContent
};


const core = require("@actions/core");
const github = require("@actions/github");
const axios = require("axios");
const fs = require("fs");
const path = require("path");
const yaml = require("js-yaml");

// Import utilities with safe fallbacks for Coalesce YML files
const { 
  extractColumnsFromYML = () => [],
  extractModelNameFromYML = () => null,
  getFileContent = () => null
} = require("./yml-parser") || {};

// Get inputs with defaults
const clientId = core.getInput("api_client_id") || "";
const clientSecret = core.getInput("api_client_secret") || "";
const changedFilesList = core.getInput("changed_files_list") || "";
const githubToken = core.getInput("github_token") || "";
const dqlabs_base_url = core.getInput("dqlabs_base_url") || "";
const dqlabs_createlink_url = core.getInput("dqlabs_createlink_url") || "";
const dqlabs_configurable_keys = core.getInput("dqlabs_configurable_keys") || "";

// Safe array processing utility
const safeArray = (maybeArray) => Array.isArray(maybeArray) ? maybeArray : [];

// Parse configurable keys
const parseConfigurableKeys = (keysString) => {
  if (!keysString || typeof keysString !== 'string') {
    return {
      showDirectColumnCount: true,
      showIndirectColumnCount: true,
      showDirectAssetCount: true,
      showIndirectAssetCount: true,
      showDirectColumnList: true,
      showIndirectColumnList: true,
      showDirectAssetList: true,
      showIndirectAssetList: true,
      showYmlColumnsAdded: true,
      showYmlColumnsRemoved: true
    };
  }

  const keys = keysString.split(',').map(key => key.trim().toLowerCase());
  
  return {
    showDirectColumnCount: keys.includes('direct_column_count'),
    showIndirectColumnCount: keys.includes('indirect_column_count'),
    showDirectAssetCount: keys.includes('direct_asset_count'),
    showIndirectAssetCount: keys.includes('indirect_asset_count'),
    showDirectColumnList: keys.includes('direct_column_list'),
    showIndirectColumnList: keys.includes('indirect_column_list'),
    showDirectAssetList: keys.includes('direct_asset_list'),
    showIndirectAssetList: keys.includes('indirect_asset_list'),
    showYmlColumnsAdded: keys.includes('yml_columns_added'),
    showYmlColumnsRemoved: keys.includes('yml_columns_removed')
  };
};

// Parse the configurable keys
const configurableKeys = parseConfigurableKeys(dqlabs_configurable_keys);

const getChangedFiles = async () => {
  try {
    if (changedFilesList && typeof changedFilesList === "string") {
      return changedFilesList
        .split(",")
        .map(f => typeof f === "string" ? f.trim() : "")
        .filter(f => f && f.length > 0);
    }

    const eventPath = process.env.GITHUB_EVENT_PATH;
    if (!eventPath) return [];

    const eventData = JSON.parse(fs.readFileSync(eventPath, "utf8"));
    const changedFiles = new Set();

    const commits = safeArray(eventData.commits);
    commits.forEach(commit => {
      if (!commit) return;
      const files = [
        ...safeArray(commit.added),
        ...safeArray(commit.modified),
        ...safeArray(commit.removed)
      ];
      files.filter(Boolean).forEach(file => changedFiles.add(file));
    });

    return Array.from(changedFiles);
  } catch (error) {
    core.error(`[getChangedFiles] Error: ${error.message}`);
    return [];
  }
};

const getTasks = async () => {
  try {
    const taskUrl = `${dqlabs_base_url}api/pipeline/task/`;
    const payload = {
      chartType: 0,
      search: {},
      page: 0,
      pageLimit: 100,
      sortBy: "name",
      orderBy: "asc",
      date_filter: { days: "All", selected: "All" },
      chart_filter: {},
      is_chart: true,
    };

    const response = await axios.post(taskUrl, payload, {
      headers: {
        "Content-Type": "application/json",
        "client-id": clientId,
        "client-secret": clientSecret,
      }
    });

    return response?.data?.response?.data || [];
  } catch (error) {
    core.error(`[getTasks] Error: ${error.message}`);
    return [];
  }
};

const getImpactAnalysisData = async (asset_id, connection_id, entity, isDirect = true) => {
  try {
    const impactAnalysisUrl = `${dqlabs_base_url}/api/lineage/impact-analysis/`;
    const payload = {
      connection_id,
      asset_id,
      entity,
      moreOptions: {
        view_by: "table",
        ...(!isDirect && { depth: 10 }) // Add depth only for indirect impact
      },
      search_key: ""
    };

    const response = await axios.post(
      impactAnalysisUrl,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": clientId,
          "client-secret": clientSecret,
        },
      }
    );

    return safeArray(response?.data?.response?.data?.tables || []);
  } catch (error) {
    core.error(`[getImpactAnalysisData] Error for ${entity}: ${error.message}`);
    return [];
  }
};

// Enhanced function for column-level impact analysis
const getColumnLevelImpactAnalysis = async (asset_id, connection_id, entity, changedColumns, isDirect = true) => {
  try {
    core.info(`[getColumnLevelImpactAnalysis] Starting analysis for entity: ${entity}, changedColumns: [${changedColumns.join(', ')}]`);
    
    const impactAnalysisUrl = `${dqlabs_base_url}/api/lineage/impact-analysis/`;
    const payload = {
      connection_id,
      asset_id,
      entity,
      field_offset: 0,
      field_limit: 200, // Increased limit to get more fields
      moreOptions: {
        view_by: "column",
        ...(!isDirect && { depth: 10 }), // Add depth only for indirect impact
      },
      search_key: ""
    };

    core.info(`[getColumnLevelImpactAnalysis] Making API call to: ${impactAnalysisUrl}`);
    core.info(`[getColumnLevelImpactAnalysis] Payload: ${JSON.stringify(payload)}`);

    const response = await axios.post(
      impactAnalysisUrl,
      payload,
      {
        headers: {
          "Content-Type": "application/json",
          "client-id": clientId,
          "client-secret": clientSecret,
        },
      }
    );

    core.info(`[getColumnLevelImpactAnalysis] API response status: ${response.status}`);
    core.info(`[getColumnLevelImpactAnalysis] Response data structure: ${JSON.stringify(Object.keys(response.data || {}))}`);

    // Extract column-level information from the response
    const tables = safeArray(response?.data?.response?.data?.tables || []);
    core.info(`[getColumnLevelImpactAnalysis] Found ${tables.length} tables in response`);
    
    const columnImpacts = [];

    tables.forEach((table, tableIndex) => {
      const fields = safeArray(table.fields || []);
      core.info(`[getColumnLevelImpactAnalysis] Table ${tableIndex + 1}: ${table.name} has ${fields.length} fields`);
      
      fields.forEach((field, fieldIndex) => {
        // Enhanced column matching with multiple strategies
        const isImpacted = changedColumns.some(changedCol => {
          const fieldName = field.name ? field.name.toLowerCase() : '';
          const changedColName = changedCol.toLowerCase();
          
          // Exact match
          if (fieldName === changedColName) return true;
          
          // Partial match (for cases where column names might be slightly different)
          if (fieldName.includes(changedColName) || changedColName.includes(fieldName)) return true;
          
          // Handle quoted column names
          const unquotedFieldName = fieldName.replace(/[`"']/g, '');
          const unquotedChangedCol = changedColName.replace(/[`"']/g, '');
          if (unquotedFieldName === unquotedChangedCol) return true;
          
          return false;
        });

        if (isImpacted) {
          core.info(`[getColumnLevelImpactAnalysis] Found impacted column: ${table.name}.${field.name}`);
          columnImpacts.push({
            table_name: table.name,
            column_name: field.name,
            column_id: field.id,
            data_type: field.data_type,
            table_id: table.id,
            redirect_id: table.redirect_id,
            entity: table.entity,
            connection_id: table.connection_id,
            asset_name: table.asset_name,
            flow: table.flow,
            depth: table.depth,
            impact_type: "Column Referenced"
          });
        }
      });
    });

    core.info(`[getColumnLevelImpactAnalysis] Found ${columnImpacts.length} column impacts for ${entity}`);
    return columnImpacts;
  } catch (error) {
    core.error(`[getColumnLevelImpactAnalysis] Error for ${entity}: ${error.message}`);
    if (error.response) {
      core.error(`[getColumnLevelImpactAnalysis] Response status: ${error.response.status}`);
      core.error(`[getColumnLevelImpactAnalysis] Response data: ${JSON.stringify(error.response.data)}`);
    }
    return [];
  }
};

// Enhanced function to extract changed columns from file changes
const extractChangedColumns = async (changedFiles) => {
  const changedColumns = {
    added: [],
    removed: [],
    modified: []
  };

  core.info(`[extractChangedColumns] Processing ${changedFiles.length} changed files`);

  for (const file of changedFiles.filter(f => f && f.endsWith(".sql"))) {
    try {
      core.info(`[extractChangedColumns] Processing file: ${file}`);
      
      const baseSha = process.env.GITHUB_BASE_SHA || github.context.payload.pull_request?.base?.sha;
      const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;

      core.info(`[extractChangedColumns] Base SHA: ${baseSha}, Head SHA: ${headSha}`);

      const baseContent = baseSha ? await getFileContent(baseSha, file) : null;
      const headContent = await getFileContent(headSha, file);
      
      if (!headContent) {
        core.warning(`[extractChangedColumns] No head content found for ${file}`);
        continue;
      }

      core.info(`[extractChangedColumns] Base content length: ${baseContent ? baseContent.length : 0}`);
      core.info(`[extractChangedColumns] Head content length: ${headContent.length}`);

      const baseCols = safeArray(baseContent ? extractColumnsFromSQL(baseContent) : []);
      const headCols = safeArray(extractColumnsFromSQL(headContent));

      core.info(`[extractChangedColumns] Base columns for ${file}: [${baseCols.join(', ')}]`);
      core.info(`[extractChangedColumns] Head columns for ${file}: [${headCols.join(', ')}]`);

      // Find added columns
      const addedCols = headCols.filter(col => !baseCols.includes(col));
      // Find removed columns
      const removedCols = baseCols.filter(col => !headCols.includes(col));

      core.info(`[extractChangedColumns] Added columns for ${file}: [${addedCols.join(', ')}]`);
      core.info(`[extractChangedColumns] Removed columns for ${file}: [${removedCols.join(', ')}]`);

      changedColumns.added.push(...addedCols.map(col => ({ column: col, file })));
      changedColumns.removed.push(...removedCols.map(col => ({ column: col, file })));
    } catch (error) {
      core.error(`[extractChangedColumns] Error extracting columns from ${file}: ${error.message}`);
      core.error(`[extractChangedColumns] Stack trace: ${error.stack}`);
    }
  }

  core.info(`[extractChangedColumns] Final results - Added: ${changedColumns.added.length}, Removed: ${changedColumns.removed.length}`);
  return changedColumns;
};

const run = async () => {
  try {
    // Initialize summary with basic info
    let summary = "## Impact Analysis Report\n\n";

    // Get changed files safely
    const changedFiles = safeArray(await getChangedFiles());
    core.info(`Found ${changedFiles.length} changed files`);

    // Extract changed columns for column-level analysis
    const changedColumns = await extractChangedColumns(changedFiles);
    core.info(`[MAIN] Found ${changedColumns.added.length} added columns and ${changedColumns.removed.length} removed columns`);
    
    // Debug: Log all changed columns
    if (changedColumns.added.length > 0) {
      core.info(`[MAIN] Added columns: ${JSON.stringify(changedColumns.added)}`);
    }
    if (changedColumns.removed.length > 0) {
      core.info(`[MAIN] Removed columns: ${JSON.stringify(changedColumns.removed)}`);
    }

    // Process changed SQL models
    const changedModels = changedFiles
      .filter(file => file && typeof file === "string" && file.endsWith(".sql"))
      .map(file => path.basename(file, path.extname(file)))
      .filter(Boolean);

    // Get tasks safely
    const tasks = await getTasks();
    core.info(`[MAIN] Retrieved ${tasks.length} tasks from DQLabs`);

    // Match tasks with changed models
    const matchedTasks = tasks
      .filter(task => task?.connection_type === "dbt")
      .filter(task => changedModels.includes(task?.name))
      .map(task => ({
        ...task,
        entity: task?.task_id || "",
        filePath: changedFiles.find(f => path.basename(f, path.extname(f)) === task.name)
      }))
      .filter(task => task.filePath); // Ensure we have the file path

    core.info(`[MAIN] Found ${matchedTasks.length} matched tasks for changed models`);
    matchedTasks.forEach(task => {
      core.info(`[MAIN] Matched task: ${task.name} (${task.entity}) -> ${task.filePath}`);
    });

    // Store impacts per file
    const fileImpacts = {};
    const columnImpacts = {}; // New structure for column-level impacts

    // Initialize file impacts structure
    matchedTasks.forEach(task => {
      fileImpacts[task.filePath] = {
        direct: [],
        indirect: [],
        taskName: task.name
      };
      columnImpacts[task.filePath] = {
        direct: [],
        indirect: [],
        taskName: task.name,
        changedColumns: []
      };
    });

    // Process impact data for each file
    for (const task of matchedTasks) {
      // Get direct impacts (without depth)
      const directImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        true // isDirect = true
      );

      // Filter out the task itself from direct impacts
      const filteredDirectImpact = directImpact
        .filter(table => table?.name !== task.name)
        .filter(Boolean);

      fileImpacts[task.filePath].direct.push(...filteredDirectImpact);

      // Get indirect impacts (with depth=10)
      const indirectImpact = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.entity,
        false // isDirect = false
      );

      fileImpacts[task.filePath].indirect.push(...indirectImpact);

      // Get column-level impacts for this task
      const taskChangedColumns = [
        ...changedColumns.added.filter(col => col.file === task.filePath).map(col => col.column),
        ...changedColumns.removed.filter(col => col.file === task.filePath).map(col => col.column)
      ];

      core.info(`[MAIN] Task ${task.name} has ${taskChangedColumns.length} changed columns: [${taskChangedColumns.join(', ')}]`);

      if (taskChangedColumns.length > 0) {
        columnImpacts[task.filePath].changedColumns = taskChangedColumns;

        core.info(`[MAIN] Getting direct column-level impacts for ${task.name}`);
        // Get direct column-level impacts
        const directColumnImpact = await getColumnLevelImpactAnalysis(
          task.asset_id,
          task.connection_id,
          task.entity,
          taskChangedColumns,
          true // isDirect = true
        );

        // Filter out the task itself from direct column impacts
        const filteredDirectColumnImpact = directColumnImpact
          .filter(column => column?.table_name !== task.name)
          .filter(Boolean);

        core.info(`[MAIN] Found ${filteredDirectColumnImpact.length} direct column impacts for ${task.name}`);
        columnImpacts[task.filePath].direct.push(...filteredDirectColumnImpact);

        core.info(`[MAIN] Getting indirect column-level impacts for ${task.name}`);
        // Get indirect column-level impacts
        const indirectColumnImpact = await getColumnLevelImpactAnalysis(
          task.asset_id,
          task.connection_id,
          task.entity,
          taskChangedColumns,
          false // isDirect = false
        );

        core.info(`[MAIN] Found ${indirectColumnImpact.length} indirect column impacts for ${task.name}`);
        columnImpacts[task.filePath].indirect.push(...indirectColumnImpact);
      } else {
        core.info(`[MAIN] No changed columns found for task ${task.name}, skipping column-level analysis`);
      }
    }

    // Create unique key function for comparison
    const uniqueKey = (item) => `${item?.name}-${item?.connection_id}-${item?.asset_name}`;

    // Remove direct impacts from indirect results for each file
    Object.keys(fileImpacts).forEach(filePath => {
      const impacts = fileImpacts[filePath];
      const directKeys = new Set(impacts.direct.map(uniqueKey));
      impacts.indirect = impacts.indirect.filter(
        item => !directKeys.has(uniqueKey(item))
      );
    });

    // Deduplicate results within each file
    const dedup = (arr) => {
      const seen = new Set();
      return arr.filter(item => {
        const key = uniqueKey(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    };

    Object.keys(fileImpacts).forEach(filePath => {
      fileImpacts[filePath].direct = dedup(fileImpacts[filePath].direct);
      fileImpacts[filePath].indirect = dedup(fileImpacts[filePath].indirect);
    });

    // Deduplicate column impacts
    const columnUniqueKey = (item) => `${item?.table_name}-${item?.column_name}-${item?.connection_id}`;
    
    Object.keys(columnImpacts).forEach(filePath => {
      const impacts = columnImpacts[filePath];
      const directKeys = new Set(impacts.direct.map(columnUniqueKey));
      impacts.indirect = impacts.indirect.filter(
        item => !directKeys.has(columnUniqueKey(item))
      );
    });

    // Deduplicate column results within each file
    const columnDedup = (arr) => {
      const seen = new Set();
      return arr.filter(item => {
        const key = columnUniqueKey(item);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    };

    Object.keys(columnImpacts).forEach(filePath => {
      columnImpacts[filePath].direct = columnDedup(columnImpacts[filePath].direct);
      columnImpacts[filePath].indirect = columnDedup(columnImpacts[filePath].indirect);
    });

    const constructItemUrl = (item, baseUrl) => {
      if (!item || !baseUrl) return "#";

      try {
        const url = new URL(baseUrl);

        // Check if we have connection_id for valid link
        if (!item.connection_id || !item.redirect_id) {
          return "#";
        }

        // Handle pipeline items
        if (item.asset_group === "pipeline") {
          if (item.is_transform) {
            url.pathname = `/observe/pipeline/transformation/${item.redirect_id}/run`;
          } else {
            url.pathname = `/observe/pipeline/task/${item.redirect_id}/run`;
          }
          return url.toString();
        }

        // Handle data items
        if (item.asset_group === "data") {
          url.pathname = `/observe/data/${item.redirect_id}/measures`;
          return url.toString();
        }

        // Default case
        return "#";
      } catch (error) {
        core.error(`Error constructing URL for ${item.name}: ${error.message}`);
        return "#";
      }
    };

    // Function to construct URLs for column-level items (using same logic as model-level)
    const constructColumnUrl = (columnItem, baseUrl) => {
      if (!columnItem || !baseUrl) return "#";

      try {
        const url = new URL(baseUrl);

        // Use the same logic as model-level links
        // Handle pipeline items
        if (columnItem.asset_group === "pipeline") {
          if (columnItem.is_transform) {
            url.pathname = `/observe/pipeline/transformation/${columnItem.redirect_id}/run`;
          } else {
            url.pathname = `/observe/pipeline/task/${columnItem.redirect_id}/run`;
          }
          return url.toString();
        }

        // Handle data items
        if (columnItem.asset_group === "data") {
          url.pathname = `/observe/data/${columnItem.redirect_id}/measures`;
          return url.toString();
        }

        // Fallback: if no connection_id or redirect_id, return non-clickable
        if (!columnItem.connection_id || !columnItem.redirect_id) {
          return "#";
        }

        // Default case for column items
        if (columnItem.redirect_id) {
          url.pathname = `/observe/pipeline/task/${columnItem.redirect_id}/run`;
          return url.toString();
        }
        
        return "#";
      } catch (error) {
        core.error(`Error constructing column URL for ${columnItem.table_name}.${columnItem.column_name}: ${error.message}`);
        return "#";
      }
    };

    // Build the complete impacts section with single collapse
    const buildImpactsSection = (fileImpacts) => {
      let content = '';
      let totalDirect = 0;
      let totalIndirect = 0;
      
      // Generate content for each file
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        const { direct, indirect, taskName } = impacts;
        totalDirect += direct.length;
        totalIndirect += indirect.length;

        content += `### File: ${filePath}\n`;
        content += `**Model:** ${taskName}\n\n`;
        
        content += `#### Directly Impacted (${direct.length})\n`;
        direct.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          const modelName = model?.name || 'Unknown';
          
          // Check if we have connection_id for clickable link
          if (model?.connection_id && url !== "#") {
            content += `- [${modelName}](${url})\n`;
          } else {
            content += `- ${modelName}\n`;
          }
        });

        content += `\n#### Indirectly Impacted (${indirect.length})\n`;
        indirect.forEach(model => {
          const url = constructItemUrl(model, dqlabs_createlink_url);
          const modelName = model?.name || 'Unknown';
          
          // Check if we have connection_id for clickable link
          if (model?.connection_id && url !== "#") {
            content += `- [${modelName}](${url})\n`;
          } else {
            content += `- ${modelName}\n`;
          }
        });

        content += '\n\n';
      });

      const totalImpacts = totalDirect + totalIndirect;
      const shouldCollapse = totalImpacts > 20;

      if (shouldCollapse) {
        return `<details>
<summary><b>Impact Analysis (${totalImpacts} total impacts - ${Object.keys(fileImpacts).length} files changed) - Click to expand</b></summary>

${content}
</details>`;
      }
      
      return content;
    };

    // Build column-level impacts section with fallback analysis
    const buildColumnImpactsSection = (columnImpacts) => {
      let content = '';
      let totalDirect = 0;
      let totalIndirect = 0;
      let hasColumnChanges = false;
      
      // Generate content for each file with column changes
      Object.entries(columnImpacts).forEach(([filePath, impacts]) => {
        const { direct, indirect, taskName, changedColumns } = impacts;
        
        if (changedColumns.length === 0) return; // Skip files with no column changes
        
        hasColumnChanges = true;
        totalDirect += direct.length;
        totalIndirect += indirect.length;

        content += `### File: ${filePath}\n`;
        content += `**Model:** ${taskName}\n`;
        content += `**Changed Columns:** ${changedColumns.join(', ')}\n\n`;
        
        if (direct.length > 0) {
          content += `#### Directly Impacted Columns (${direct.length})\n`;
          direct.forEach(column => {
            const url = constructColumnUrl(column, dqlabs_createlink_url);
            const columnName = `${column?.table_name || 'Unknown'}.${column?.column_name || 'Unknown'}`;
            
            // Check if we have connection_id for clickable link
            if (column?.connection_id && url !== "#") {
              content += `- [${columnName}](${url}) - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})\n`;
            } else {
              content += `- ${columnName} - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})\n`;
            }
          });
        } else {
          content += `#### Directly Impacted Columns (0)\n`;
          content += `*No direct column impacts detected via DQLabs API*\n`;
        }

        if (indirect.length > 0) {
          content += `\n#### Indirectly Impacted Columns (${indirect.length})\n`;
          indirect.forEach(column => {
            const url = constructColumnUrl(column, dqlabs_createlink_url);
            const columnName = `${column?.table_name || 'Unknown'}.${column?.column_name || 'Unknown'}`;
            
            // Check if we have connection_id for clickable link
            if (column?.connection_id && url !== "#") {
              content += `- [${columnName}](${url}) - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})\n`;
            } else {
              content += `- ${columnName} - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})\n`;
            }
          });
        } else {
          content += `\n#### Indirectly Impacted Columns (0)\n`;
          content += `*No indirect column impacts detected via DQLabs API*\n`;
        }

        content += '\n\n';
      });

      // If we have column changes but no impacts detected, provide a more informative message
      if (hasColumnChanges && totalDirect === 0 && totalIndirect === 0) {
        return `## Column-Level Impact Analysis\n\n**⚠️ Column changes detected but no impacts found via DQLabs API.**\n\nThis could indicate:\n- The DQLabs lineage data may not be up-to-date\n- Column-level lineage might not be fully configured\n- The changed columns may not have downstream dependencies\n- API connectivity or authentication issues\n\n**Recommendation:** Check the DQLabs platform directly to verify column-level impacts.\n\n`;
      }

      if (!hasColumnChanges) {
        return '## Column-Level Impact Analysis\n\n**No column changes detected in SQL files.**\n\n';
      }

      const totalImpacts = totalDirect + totalIndirect;
      const shouldCollapse = totalImpacts > 15;

      if (shouldCollapse) {
        return `<details>
<summary><b>Column-Level Impact Analysis (${totalImpacts} total column impacts - ${Object.keys(columnImpacts).filter(f => columnImpacts[f].changedColumns.length > 0).length} files with column changes) - Click to expand</b></summary>

${content}
</details>`;
      }
      
      return `## Column-Level Impact Analysis\n\n${content}`;
    };

    // Add impacts to summary
    summary += buildImpactsSection(fileImpacts);
    
    // Add column-level impacts to summary
    summary += buildColumnImpactsSection(columnImpacts);
    
    // Add summary of total impacts
    const totalDirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
    const totalIndirect = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
    
    // Add column-level impact statistics
    const totalColumnDirect = Object.values(columnImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
    const totalColumnIndirect = Object.values(columnImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
    const filesWithColumnChanges = Object.keys(columnImpacts).filter(f => columnImpacts[f].changedColumns.length > 0).length;
    
    summary += `\n## Summary of Impacts\n`;
    summary += `### Model-Level Impacts\n`;
    summary += `- **Total Directly Impacted:** ${totalDirect}\n`;
    summary += `- **Total Indirectly Impacted:** ${totalIndirect}\n`;
    summary += `- **Files Changed:** ${Object.keys(fileImpacts).length}\n\n`;
    summary += `### Column-Level Impacts\n`;
    summary += `- **Total Directly Impacted Columns:** ${totalColumnDirect}\n`;
    summary += `- **Total Indirectly Impacted Columns:** ${totalColumnIndirect}\n`;
    summary += `- **Files with Column Changes:** ${filesWithColumnChanges}\n\n`;

    // Process column changes
    const processColumnChanges = async (extension, extractor, isYml = false) => {
      const changes = [];
      let added = [];
      let removed = [];

      for (const file of changedFiles.filter(f => f && f.endsWith(extension))) {
        try {
          const baseSha = process.env.GITHUB_BASE_SHA || github.context.payload.pull_request?.base?.sha;
          const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;

          const baseContent = baseSha ? await getFileContent(baseSha, file) : null;
          const headContent = await getFileContent(headSha, file);
          if (!headContent) continue;

          const baseCols = safeArray(baseContent ? extractor(baseContent, file) : []);
          const headCols = safeArray(extractor(headContent, file));

          // Handle YML columns differently
          if (isYml) {
            // Extract just the names for comparison
            const baseColNames = baseCols.map(col => col.name);
            const headColNames = headCols.map(col => col.name);

            const addedCols = headCols.filter(col => !baseColNames.includes(col.name));
            const removedCols = baseCols.filter(col => !headColNames.includes(col.name));

            // Get full column info for added/removed
            added.push(...addedCols);
            removed.push(...removedCols);

            if (addedCols.length > 0 || removedCols.length > 0) {
              changes.push({ 
                file, 
                added: addedCols.map(c => c.name),
                removed: removedCols.map(c => c.name)
              });
            }
          } else {
            // Original SQL comparison logic
            const addedCols = headCols.filter(col => !baseCols.includes(col));
            const removedCols = baseCols.filter(col => !headCols.includes(col));

            added.push(...addedCols);
            removed.push(...removedCols);

            if (addedCols.length > 0 || removedCols.length > 0) {
              changes.push({ file, added: addedCols, removed: removedCols });
            }
          }
        } catch (error) {
          core.error(`Error processing ${file}: ${error.message}`);
        }
      }

      return { changes, added, removed };
    };

    // Process SQL changes
    const { added: sqlAdded, removed: sqlRemoved } = await processColumnChanges(".sql", extractColumnsFromSQL);
    summary += `\n### SQL Column Changes\n`;
    summary += `Added columns(${sqlAdded.length}): ${sqlAdded.join(', ')}\n`;
    summary += `Removed columns(${sqlRemoved.length}): ${sqlRemoved.join(', ')}\n`;

    // Process YML changes
    const { added: ymlAdded, removed: ymlRemoved } = await processColumnChanges(".yml", (content, file) => extractColumnsFromYML(content, file), true);
    summary += `\n### YML Column Changes\n`;
    summary += `Added columns(${ymlAdded.length}): ${ymlAdded.map(c => c.name).join(', ')}\n`;
    summary += `Removed columns(${ymlRemoved.length}): ${ymlRemoved.map(c => c.name).join(', ')}\n`;

    // Post comment
    if (github.context.payload.pull_request) {
      try {
        const octokit = github.getOctokit(githubToken);
        await octokit.rest.issues.createComment({
          owner: github.context.repo.owner,
          repo: github.context.repo.repo,
          issue_number: github.context.payload.pull_request.number,
          body: summary,
        });
      } catch (error) {
        core.error(`Failed to create comment: ${error.message}`);
      }
    }

    // Output results
    await core.summary
      .addRaw(summary)
      .write();

    core.setOutput("impact_markdown", summary);
  } catch (error) {
    core.setFailed(`[MAIN] Unhandled error: ${error.message}`);
    core.error(error.stack);
  }
};

// Execute
run().catch(error => {
  core.setFailed(`[UNCAUGHT] Critical failure: ${error.message}`);
});

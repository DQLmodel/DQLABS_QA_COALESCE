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
      showYmlColumnChanges: true
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
    showYmlColumnChanges: keys.includes('yml_column_changes')
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
    const taskUrl = `${dqlabs_base_url}api/pipeline/job/`;
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
      search_key: "",
      is_github: true
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
    return response?.data?.response?.data || {};
  } catch (error) {
    core.error(`[getImpactAnalysisData] Error for ${entity}: ${error.message}`);
    return {};
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
            impact_type: "Column Referenced",
            asset_group: table.asset_group
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

// Enhanced function to extract changed columns from YML file changes
const extractChangedColumns = async (changedFiles) => {
  const changedColumns = {
    added: [],
    removed: [],
    modified: []
  };

  core.info(`[extractChangedColumns] Processing ${changedFiles.length} changed files`);

  for (const file of changedFiles.filter(f => f && f.endsWith(".yml"))) {
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

      const baseCols = safeArray(baseContent ? extractColumnsFromYML(baseContent, file) : []);
      const headCols = safeArray(extractColumnsFromYML(headContent, file));

      // Extract just the names for comparison
      const baseColNames = baseCols.map(col => col.name);
      const headColNames = headCols.map(col => col.name);

      core.info(`[extractChangedColumns] Base columns for ${file}: [${baseColNames.join(', ')}]`);
      core.info(`[extractChangedColumns] Head columns for ${file}: [${headColNames.join(', ')}]`);

      // Find added columns
      const addedCols = headCols.filter(col => !baseColNames.includes(col.name));
      // Find removed columns
      const removedCols = baseCols.filter(col => !headColNames.includes(col.name));

      core.info(`[extractChangedColumns] Added columns for ${file}: [${addedCols.map(c => c.name).join(', ')}]`);
      core.info(`[extractChangedColumns] Removed columns for ${file}: [${removedCols.map(c => c.name).join(', ')}]`);

      changedColumns.added.push(...addedCols.map(col => ({ column: col.name, file })));
      changedColumns.removed.push(...removedCols.map(col => ({ column: col.name, file })));
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
    
    // Log YML files for debugging
    const ymlFiles = changedFiles.filter(f => f && f.endsWith(".yml"));
    if (ymlFiles.length > 0) {
      core.info(`Found ${ymlFiles.length} YML files: [${ymlFiles.join(', ')}]`);
    } else {
      core.warning(`No YML files found in changed files. All files: [${changedFiles.join(', ')}]`);
    }

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

    // Process changed YML files (Coalesce nodes)
    const changedYmlFiles = changedFiles
      .filter(file => file && typeof file === "string" && file.endsWith(".yml"))
      .filter(Boolean);

    // Extract model names from YML files
    const changedModels = [];
    const modelNameToFileMap = {};
    
    core.info(`[MAIN] Processing ${changedYmlFiles.length} changed YML files`);
    
    for (const file of changedYmlFiles) {
      try {
        const headSha = process.env.GITHUB_HEAD_SHA || github.context.payload.pull_request?.head?.sha;
        if (!headSha) {
          core.warning(`[MAIN] No head SHA found, trying to read file directly: ${file}`);
        }
        
        const headContent = await getFileContent(headSha, file);
        if (!headContent) {
          core.warning(`[MAIN] Could not read content for ${file}, trying to read from filesystem`);
          // Fallback: try reading from filesystem
          try {
            const fsContent = fs.readFileSync(file, 'utf8');
            if (fsContent) {
              const modelName = extractModelNameFromYML(fsContent, file);
              if (modelName) {
                changedModels.push(modelName);
                modelNameToFileMap[modelName] = file;
                core.info(`[MAIN] Extracted model name '${modelName}' from ${file} (via filesystem)`);
              }
            }
          } catch (fsError) {
            core.error(`[MAIN] Could not read ${file} from filesystem: ${fsError.message}`);
          }
          continue;
        }
        
        const modelName = extractModelNameFromYML(headContent, file);
        if (modelName) {
          changedModels.push(modelName);
          modelNameToFileMap[modelName] = file;
          core.info(`[MAIN] Extracted model name '${modelName}' from ${file}`);
        } else {
          core.warning(`[MAIN] Could not extract model name from ${file}`);
        }
      } catch (error) {
        core.error(`[MAIN] Error extracting model name from ${file}: ${error.message}`);
        core.error(`[MAIN] Stack trace: ${error.stack}`);
      }
    }

    core.info(`[MAIN] Found ${changedModels.length} changed models from YML files: [${changedModels.join(', ')}]`);

    // Get tasks safely
    const tasks = await getTasks();
    core.info(`[MAIN] Retrieved ${tasks.length} tasks from DQLabs`);

    // Match tasks with changed models (filter for Coalesce connection type)
    // Use case-insensitive matching for connection_type
    const coalesceTasks = tasks.filter(task => {
      const connType = (task?.connection_type || "").toLowerCase();
      return connType === "coalesce_pipeline" || connType.includes("coalesce_pipeline");
    });
    
    core.info(`[MAIN] Found ${coalesceTasks.length} Coalesce tasks out of ${tasks.length} total tasks`);
    
    // Log all Coalesce task names for debugging
    if (coalesceTasks.length > 0) {
      core.info(`[MAIN] Coalesce task names: [${coalesceTasks.map(t => t.name).join(', ')}]`);
    }
    
    // Use case-insensitive matching for model names
    const matchedTasks = coalesceTasks
      .filter(task => {
        const taskName = (task?.name || "").toLowerCase();
        return changedModels.some(model => model.toLowerCase() === taskName);
      })
      .map(task => {
        // Find matching model (case-insensitive)
        const matchingModel = changedModels.find(model => 
          model.toLowerCase() === (task?.name || "").toLowerCase()
        );
        return {
          ...task,
          entity: task?.task_id || "",
          filePath: matchingModel ? modelNameToFileMap[matchingModel] : null
        };
      })
      .filter(task => task.filePath); // Ensure we have the file path

    core.info(`[MAIN] Found ${matchedTasks.length} matched tasks for changed models`);
    if (matchedTasks.length === 0 && changedModels.length > 0) {
      core.warning(`[MAIN] No tasks matched! Changed models: [${changedModels.join(', ')}]`);
      core.warning(`[MAIN] Available Coalesce tasks: [${coalesceTasks.map(t => t.name).join(', ')}]`);
    }
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
      // Get impact data (with depth for indirect)
      const impactData = await getImpactAnalysisData(
        task.asset_id,
        task.connection_id,
        task.asset_id,
        false // isDirect = false to get both direct and indirect
      );

      const impactTables = impactData?.direct || [];
      const indirectImpact = impactData?.indirect || [];
      const indirectNameSet = new Set(indirectImpact.map(item => item?.name));

      // Filter out the task itself from direct impacts and remove items that are in indirect
      const filteredDirectImpact = impactTables
        .filter(table => !indirectNameSet.has(table?.name))
        .filter(table => table?.name !== task.name)
        .filter(Boolean);

      fileImpacts[task.filePath].direct.push(...filteredDirectImpact);

      // Filter out the task itself from indirect impacts
      const filteredInDirectImpact = indirectImpact
        .filter(table => table?.name !== task.name)
        .filter(Boolean);

      fileImpacts[task.filePath].indirect.push(...filteredInDirectImpact);
      core.info(`Directly impacted assets: ${JSON.stringify(filteredDirectImpact.map(asset => ({
        name: asset.name,
        connection_id: asset.connection_id,
        asset_name: asset.asset_name,
        asset_group: asset.asset_group
      })), null, 2)}`);

      // For indirect impacts  
      core.info(`Indirectly impacted assets: ${JSON.stringify(filteredInDirectImpact.map(asset => ({
        name: asset.name,
        connection_id: asset.connection_id,
        asset_name: asset.asset_name,
        asset_group: asset.asset_group
      })), null, 2)}`);

      // For file impacts structure
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        core.info(`File: ${filePath}`);
        core.info(`- Direct impacts: ${impacts.direct.length} assets`);
        core.info(`- Indirect impacts: ${impacts.indirect.length} assets`);
        
        if (impacts.direct.length > 0) {
          core.info(`  Direct assets: ${JSON.stringify(impacts.direct.map(a => a.name))}`);
        }
        if (impacts.indirect.length > 0) {
          core.info(`  Indirect assets: ${JSON.stringify(impacts.indirect.map(a => a.name))}`);
        }
      });
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
          task.asset_id,
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
          task.asset_id,
          taskChangedColumns,
          false // isDirect = false
        );

        core.info(`[MAIN] Found ${indirectColumnImpact.length} indirect column impacts for ${task.name}`);
        const filteredInDirectColumnImpact = indirectColumnImpact
          .filter(column => column?.table_name !== task.name)
          .filter(Boolean);
        columnImpacts[task.filePath].indirect.push(...filteredInDirectColumnImpact);
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

        // Handle report items
        if (item.asset_group === "report") {
          url.pathname = `/observe/report/worksheet/${item.redirect_id}/overview`;
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

    // Build the new simplified report structure
    const buildNewAnalysisReport = (fileImpacts, columnImpacts, changedFiles) => {
      let report = "## Impact Analysis Report\n\n";
      
      // 1. Changed Files section (always show)
      report += "### Changed Files\n";
      if (changedFiles.length > 0) {
        changedFiles.forEach(file => {
          report += `- ${file}\n`;
        });
      } else {
        report += "- No files changed\n";
      }
      report += "\n";
      
      // 2. Asset level Impacts section (only if asset keys are requested)
      const hasAssetKeys = configurableKeys.showDirectAssetCount || configurableKeys.showIndirectAssetCount || 
                           configurableKeys.showDirectAssetList || configurableKeys.showIndirectAssetList;
      
      if (hasAssetKeys) {
        report += "### Asset level Impacts\n";
        
        // Calculate totals
        const totalDirectAssets = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
        const totalIndirectAssets = Object.values(fileImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
        
        // Show count keys first
        if (configurableKeys.showDirectAssetCount) {
          report += `- **Total Directly Impacted:** ${totalDirectAssets}\n`;
        }
        if (configurableKeys.showIndirectAssetCount) {
          report += `- **Total Indirectly Impacted:** ${totalIndirectAssets}\n`;
        }
        
        // Show list keys second (as collapsible sections)
        if (configurableKeys.showDirectAssetList) {
          const directAssets = [];
          Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
            impacts.direct.forEach(model => {
              const url = constructItemUrl(model, dqlabs_createlink_url);
              const modelName = model?.name || 'Unknown';
              if (model?.connection_id && url !== "#") {
                directAssets.push(`- [${modelName}](${url})`);
              } else {
                directAssets.push(`- ${modelName}`);
              }
            });
          });
          
          if (directAssets.length > 0) {
            report += `\n<details>\n<summary><b>Directly Impacted Assets (${directAssets.length})</b></summary>\n\n`;
            report += directAssets.join('\n') + '\n';
            report += `</details>\n`;
          }
        }
        
        if (configurableKeys.showIndirectAssetList) {
          const indirectAssets = [];
          Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
            impacts.indirect.forEach(model => {
              const url = constructItemUrl(model, dqlabs_createlink_url);
              const modelName = model?.name || 'Unknown';
              if (model?.connection_id && url !== "#") {
                indirectAssets.push(`- [${modelName}](${url})`);
              } else {
                indirectAssets.push(`- ${modelName}`);
              }
            });
          });
          
          if (indirectAssets.length > 0) {
            report += `\n<details>\n<summary><b>Indirectly Impacted Assets (${indirectAssets.length})</b></summary>\n\n`;
            report += indirectAssets.join('\n') + '\n';
            report += `</details>\n`;
          }
        }
        
        report += "\n";
      }
      
      // 3. Column level Impacts section (only if column keys are requested)
      const hasColumnKeys = configurableKeys.showDirectColumnCount || configurableKeys.showIndirectColumnCount || 
                           configurableKeys.showDirectColumnList || configurableKeys.showIndirectColumnList;
      
      if (hasColumnKeys) {
        report += "### Column level Impacts\n";
        
        // Calculate totals
        const totalDirectColumns = Object.values(columnImpacts).reduce((sum, impacts) => sum + impacts.direct.length, 0);
        const totalIndirectColumns = Object.values(columnImpacts).reduce((sum, impacts) => sum + impacts.indirect.length, 0);
        
        // Show count keys first
        if (configurableKeys.showDirectColumnCount) {
          report += `- **Total Directly Impacted Columns:** ${totalDirectColumns}\n`;
        }
        if (configurableKeys.showIndirectColumnCount) {
          report += `- **Total Indirectly Impacted Columns:** ${totalIndirectColumns}\n`;
        }
        
        // Show list keys second (as collapsible sections)
        if (configurableKeys.showDirectColumnList) {
          const directColumns = [];
          Object.entries(columnImpacts).forEach(([filePath, impacts]) => {
            impacts.direct.forEach(column => {
              const url = constructColumnUrl(column, dqlabs_createlink_url);
              const columnName = `${column?.table_name || 'Unknown'}.${column?.column_name || 'Unknown'}`;
              if (column?.connection_id && url !== "#") {
                directColumns.push(`- [${columnName}](${url}) - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})`);
              } else {
                directColumns.push(`- ${columnName} - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})`);
              }
            });
          });
          
          if (directColumns.length > 0) {
            report += `\n<details>\n<summary><b>Directly Impacted Columns (${directColumns.length})</b></summary>\n\n`;
            report += directColumns.join('\n') + '\n';
            report += `</details>\n`;
          }
        }
        
        if (configurableKeys.showIndirectColumnList) {
          const indirectColumns = [];
          Object.entries(columnImpacts).forEach(([filePath, impacts]) => {
            impacts.indirect.forEach(column => {
              const url = constructColumnUrl(column, dqlabs_createlink_url);
              const columnName = `${column?.table_name || 'Unknown'}.${column?.column_name || 'Unknown'}`;
              if (column?.connection_id && url !== "#") {
                indirectColumns.push(`- [${columnName}](${url}) - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})`);
              } else {
                indirectColumns.push(`- ${columnName} - *${column?.impact_type || 'Referenced'}* (${column?.data_type || 'Unknown Type'})`);
              }
            });
          });
          
          if (indirectColumns.length > 0) {
            report += `\n<details>\n<summary><b>Indirectly Impacted Columns (${indirectColumns.length})</b></summary>\n\n`;
            report += indirectColumns.join('\n') + '\n';
            report += `</details>\n`;
          }
        }
        
        report += "\n";
      }
      
      return report;
    };


    // Process column changes function for YML files
    const processColumnChanges = async (extension, extractor) => {
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

          // Handle YML columns (Coalesce format)
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
        } catch (error) {
          core.error(`Error processing ${file}: ${error.message}`);
        }
      }

      return { changes, added, removed };
    };

    // Process YML column changes only (Coalesce YML files)
    const { added: ymlAdded, removed: ymlRemoved } = await processColumnChanges(".yml", (content, file) => extractColumnsFromYML(content, file));
    
    // Build the new simplified report
    summary = buildNewAnalysisReport(fileImpacts, columnImpacts, changedFiles);
    
    // Add YML Column Changes section (conditional)
    if (configurableKeys.showYmlColumnChanges) {
      summary += "### YML Column Changes\n";
      summary += `Added columns(${ymlAdded.length}): ${ymlAdded.map(c => c.name).join(', ')}\n`;
      summary += `Removed columns(${ymlRemoved.length}): ${ymlRemoved.map(c => c.name).join(', ')}\n\n`;
    }

    // Generate comprehensive JSON file with all data (regardless of configurable keys)
    const generateComprehensiveJSON = (fileImpacts, columnImpacts, changedFiles, ymlAdded, ymlRemoved) => {
      const jsonData = {
        metadata: {
          timestamp: new Date().toISOString(),
          commit_sha: github.context.sha,
          pull_request_number: github.context.payload.pull_request?.number || null,
          configurable_keys_used: dqlabs_configurable_keys ? dqlabs_configurable_keys.split(',').map(k => k.trim()) : [],
          dqlabs_base_url: dqlabs_base_url,
          analysis_type: "coalesce_yml_impact_analysis"
        },
        changed_files: changedFiles,
        asset_impacts: {
          direct: [],
          indirect: []
        },
        column_impacts: {
          direct: [],
          indirect: []
        },
        yml_column_changes: {
          added: ymlAdded.map(c => c.name),
          removed: ymlRemoved.map(c => c.name)
        },
        summary: {
          total_direct_assets: 0,
          total_indirect_assets: 0,
          total_direct_columns: 0,
          total_indirect_columns: 0,
          total_yml_added: ymlAdded.length,
          total_yml_removed: ymlRemoved.length,
          total_changed_files: changedFiles.length
        }
      };

      // Process file impacts
      Object.entries(fileImpacts).forEach(([filePath, impacts]) => {
        impacts.direct.forEach(model => {
          const redirectUrl = constructItemUrl(model, dqlabs_createlink_url);
          jsonData.asset_impacts.direct.push({
            file_path: filePath,
            model_name: model.name,
            task_name: impacts.taskName,
            redirect_url: redirectUrl
          });
        });

        impacts.indirect.forEach(model => {
          const redirectUrl = constructItemUrl(model, dqlabs_createlink_url);
          jsonData.asset_impacts.indirect.push({
            file_path: filePath,
            model_name: model.name,
            task_name: impacts.taskName,
            redirect_url: redirectUrl
          });
        });
      });

      // Process column impactsa
      Object.entries(columnImpacts).forEach(([filePath, impacts]) => {
        impacts.direct.forEach(column => {
          const redirectUrl = constructColumnUrl(column, dqlabs_createlink_url);
          jsonData.column_impacts.direct.push({
            file_path: filePath,
            table_name: column.table_name,
            column_name: column.column_name,
            data_type: column.data_type,
            task_name: impacts.taskName,
            redirect_url: redirectUrl
          });
        });

        impacts.indirect.forEach(column => {
          const redirectUrl = constructColumnUrl(column, dqlabs_createlink_url);
          jsonData.column_impacts.indirect.push({
            file_path: filePath,
            table_name: column.table_name,
            column_name: column.column_name,
            data_type: column.data_type,
            task_name: impacts.taskName,
            redirect_url: redirectUrl
          });
        });
      });

      // Calculate summary totals
      jsonData.summary.total_direct_assets = jsonData.asset_impacts.direct.length;
      jsonData.summary.total_indirect_assets = jsonData.asset_impacts.indirect.length;
      jsonData.summary.total_direct_columns = jsonData.column_impacts.direct.length;
      jsonData.summary.total_indirect_columns = jsonData.column_impacts.indirect.length;

      return JSON.stringify(jsonData, null, 2);
    };

    // Generate comprehensive JSON data
    const comprehensiveJsonData = generateComprehensiveJSON(fileImpacts, columnImpacts, changedFiles, ymlAdded, ymlRemoved);

    // Post or update comment
    if (github.context.payload.pull_request) {
      try {
        const octokit = github.getOctokit(githubToken);
        const { owner, repo } = github.context.repo;
        const issue_number = github.context.payload.pull_request.number;
        
        // Get existing comments to find our bot's comment
        const comments = await octokit.rest.issues.listComments({
          owner,
          repo,
          issue_number,
        });
        
        // Find existing comment from github-actions[bot] with our impact analysis
        const existingComment = comments.data.find(comment => 
          comment.user.type === 'Bot' && 
          comment.user.login === 'github-actions[bot]' &&
          comment.body.includes('## Impact Analysis Report')
        );
        
        // Add JSON data as collapsible section
        let finalSummary = summary;
        finalSummary += "\n### 📎 Complete Impact Analysis Data\n";
        finalSummary += `<details>\n<summary><b>View Complete JSON Data</b></summary>\n\n`;
        finalSummary += "```json\n";
        finalSummary += comprehensiveJsonData;
        finalSummary += "\n```\n\n";
        finalSummary += "*This JSON contains all impact analysis data regardless of display preferences.*\n";
        finalSummary += `</details>\n\n`;
        
        // Create or update comment with the JSON data
        if (existingComment) {
          core.info(`Updating existing comment ${existingComment.id} with JSON data`);
          await octokit.rest.issues.updateComment({
            owner,
            repo,
            comment_id: existingComment.id,
            body: finalSummary,
          });
          core.info('Successfully updated existing impact analysis comment');
        } else {
          core.info('Creating new impact analysis comment with JSON data');
          await octokit.rest.issues.createComment({
            owner,
            repo,
            issue_number,
            body: finalSummary,
          });
          core.info('Successfully created new impact analysis comment');
        }
        
      } catch (error) {
        core.error(`Failed to post/update comment: ${error.message}`);
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

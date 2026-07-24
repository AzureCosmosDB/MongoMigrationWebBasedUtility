using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Microsoft.JSInterop;
using OnlineMongoMigrationProcessor;
using OnlineMongoMigrationProcessor.Models;
using OnlineMongoMigrationProcessor.Helpers.Mongo;
using OnlineMongoMigrationProcessor.Context;
using MongoMigrationWebApp.Models;
using MongoMigrationWebApp.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.RegularExpressions;

namespace MongoMigrationWebApp.Components
{
    public partial class ManageCollections : ComponentBase
    {
        private bool IsRuOptimizedCopyJob => MigrationJob?.JobType == JobType.RUOptimizedCopy;

        // Sync Only jobs skip the data copy entirely, so dropping/overwriting the target is not
        // meaningful — only append is allowed. Overwrite is forced FALSE across Add/Edit and Bulk.
        private bool IsSyncOnlyJob => MigrationJob?.CDCMode == CDCMode.SyncOnly;

        // Curated list of _id BSON types the user can pin (excludes the catch-all DataType.Other).
        // Null/empty selection in the UI represents "Unknown / Multiple" and leaves DataTypeForId unset.
        private static readonly DataType[] IdDataTypeOptions = new[]
        {
            DataType.ObjectId,
            DataType.Int,
            DataType.Int64,
            DataType.Double,
            DataType.Decimal128,
            DataType.Date,
            DataType.Timestamp,
            DataType.BinData,
            DataType.String,
            DataType.Object,
        };

        [Parameter, EditorRequired]
        public MigrationJob MigrationJob { get; set; } = null!;

        [Parameter, EditorRequired]
        public List<MigrationUnit> MigrationUnits { get; set; } = null!;

        [Parameter, EditorRequired]
        public string SourceConnectionString { get; set; } = string.Empty;

        [Parameter, EditorRequired]
        public string TargetConnectionString { get; set; } = string.Empty;

        [Parameter]
        public EventCallback OnCollectionsCommitted { get; set; }

        [Parameter]
        public EventCallback OnCancelled { get; set; }

        // Fired once after the component's first render so a parent can dismiss
        // a "loading" overlay shown while this dialog initialises.
        [Parameter]
        public EventCallback OnReady { get; set; }

        [Inject]
        private IJSRuntime JS { get; set; } = null!;

        // State fields per spec section 4
        private List<MigrationUnit> _liveUnits = new();
        private List<PendingAddition> _drafts = new();
        private HashSet<string> _toRemoveIds = new();
        private List<string> _clusterNodes = new();
        private bool _loadingNodes;
        private string? _error;
        private Guid? _expandedDraftId;
        private bool _showAddPanel;
        private bool _showSummary;
        private bool _isApplying;
        private bool _isPreparingSummary;

        // For Add/Edit form state
        private string _formNamespaces = string.Empty;
        private bool? _formOverwrite = false;
        private IndexingStrategy? _formIndexing = IndexingStrategy.SameAsSource;
        private ShardingStrategy? _formSharding = ShardingStrategy.DontShard;
        private string? _formMoveToShard = "Auto";
        private string? _formFilter = null;
        // null = "Unknown / Multiple" (let partitioner detect).
        private DataType? _formDataTypeForId = null;
        // Set when an edit panel just opened, so OnAfterRender can scroll it into view.
        private Guid? _pendingScrollDraftId;

        // Selection state
        private HashSet<string> _selectedLiveIds = new();
        private HashSet<Guid> _selectedDraftIds = new();

        // Pagination
        private PaginationHelper<object> _paginationHelper = null!;

        // Type filter for the list ("all", "existing", "new", "remove")
        private string _typeFilter = "all";

        // Sortable columns: "namespace" or "status"
        private string _sortColumn = "namespace";
        private bool _sortAscending = true;

        protected override void OnInitialized()
        {
            _liveUnits = MigrationUnits.ToList();

            // Initialize pagination with merged list (live units + drafts + pending removals).
            // Type filter is applied at the source level via GetAllRowItems() so it works even
            // when the search box is empty (PaginationHelper short-circuits the predicate then).
            _paginationHelper = new PaginationHelper<object>(
                GetSortedRowItems(),
                pageSize: 25,
                filterPredicate: (item, filter) =>
                {
                    if (string.IsNullOrWhiteSpace(filter)) return true;
                    var ns = GetNamespace(item);
                    var status = GetStatusSearchText(item);
                    return ns.Contains(filter, StringComparison.OrdinalIgnoreCase) ||
                           status.Contains(filter, StringComparison.OrdinalIgnoreCase);
                }
            );
        }

        protected override async Task OnInitializedAsync()
        {
            // Preload target cluster shards so single-shard targets can immediately disable the
            // Move-to-shard dropdown and pick the correct default before the user opens any panel.
            await EnsureClusterNodesLoaded();
        }

        private bool IsSingleShardTarget => _clusterNodes.Count <= 1;

        private bool MatchesTypeFilter(object item)
        {
            return _typeFilter switch
            {
                "existing" => item is MigrationUnit u && !_toRemoveIds.Contains(u.Id),
                "new" => item is PendingAddition,
                "remove" => item is MigrationUnit ur && _toRemoveIds.Contains(ur.Id),
                _ => true
            };
        }

        private void OnTypeFilterChanged(ChangeEventArgs e)
        {
            _typeFilter = e.Value?.ToString() ?? "all";
            RefreshPagination();
        }

        private List<object> GetAllRowItems()
        {
            var items = new List<object>();
            items.AddRange(_liveUnits.Cast<object>());
            items.AddRange(_drafts.Cast<object>());
            return items.Where(MatchesTypeFilter).ToList();
        }

        private string GetNamespace(object item)
        {
            return item switch
            {
                MigrationUnit u => $"{u.DatabaseName}.{u.CollectionName}",
                PendingAddition d => $"{d.DatabaseName}.{d.CollectionName}",
                _ => string.Empty
            };
        }

        // Combined searchable/sortable status string covering both the state badge and the source status.
        private string GetStatusSearchText(object item)
        {
            if (item is MigrationUnit u)
            {
                return _toRemoveIds.Contains(u.Id)
                    ? "Remove"
                    : $"Existing {GetUnitStatusDisplay(u)}";
            }
            if (item is PendingAddition)
            {
                return "New Pending";
            }
            return string.Empty;
        }

        // Mirrors the status text shown in MigrationJobViewer so users see the same wording
        // (e.g. "Skipped (Max Retries)") and can filter/sort by it.
        private static string GetUnitStatusDisplay(MigrationUnit u)
        {
            if (u.SourceStatus == CollectionStatus.NotFound) return "Skipped (404)";
            if (u.SourceStatus == CollectionStatus.IsView) return "Skipped (VIEW)";
            if (u.SourceStatus == CollectionStatus.IsTimeSeries) return "Skipped (TIMESERIES)";
            if (u.SkippedDueToMaxRetries) return "Skipped (Max Retries)";
            return u.SourceStatus.ToString();
        }

        private List<object> GetSortedRowItems()
        {
            var items = GetAllRowItems();
            Func<object, string> keySelector = _sortColumn == "status" ? GetStatusSearchText : GetNamespace;
            var ordered = _sortAscending
                ? items.OrderBy(keySelector, StringComparer.OrdinalIgnoreCase)
                : items.OrderByDescending(keySelector, StringComparer.OrdinalIgnoreCase);
            return ordered.ToList();
        }

        private void OnSort(string column)
        {
            if (_sortColumn == column)
            {
                _sortAscending = !_sortAscending;
            }
            else
            {
                _sortColumn = column;
                _sortAscending = true;
            }
            RefreshPagination();
        }

        private string GetSortIcon(string column)
        {
            if (_sortColumn != column) return "bi-arrow-down-up text-muted";
            return _sortAscending ? "bi-arrow-up" : "bi-arrow-down";
        }

        private void RefreshPagination()
        {
            _paginationHelper.UpdateSource(GetSortedRowItems());
            StateHasChanged();
        }

        // === LIST VIEW METHODS ===

        private async Task ToggleAddPanel()
        {
            _showAddPanel = !_showAddPanel;
            // Clear any stale alert (success/skipped/error) from a previous action so the user
            // starts with a clean panel each time it opens or closes.
            _error = null;
            if (_showAddPanel)
            {
                ResetAddForm();
                _expandedDraftId = null; // Close any open edit panel
                // Default sharding is DontShard, so the Move-to dropdown is visible immediately; preload nodes.
                await EnsureClusterNodesLoaded();
            }
        }

        private void ResetAddForm()
        {
            _formNamespaces = string.Empty;
            _formOverwrite = false;
            _formIndexing = IndexingStrategy.SameAsSource;
            _formSharding = ShardingStrategy.DontShard;
            _formMoveToShard = IsSingleShardTarget ? null : "Auto";
            _formFilter = null;
            _formDataTypeForId = null;
        }

        private async Task AddCollections()
        {
            _error = null;

            if (string.IsNullOrWhiteSpace(_formNamespaces))
            {
                _error = "Please enter at least one namespace.";
                return;
            }

            // Validate filter if provided. Use the MongoDB parser (relaxed/extended JSON) so the
            // validation matches what the migration pipeline actually uses at runtime — this accepts
            // query operators like $exists/$gte, unquoted field names, and extended-JSON types.
            if (!IsRuOptimizedCopyJob && !string.IsNullOrWhiteSpace(_formFilter))
            {
                if (!MongoHelper.IsValidFilter(_formFilter, out var filterError))
                {
                    _error = $"Invalid filter: {filterError}";
                    return;
                }
            }

            // Validate the user input using the shared helper so CSV (comma-separated),
            // wildcard (db.*, *.col, *.*), and JSON-array forms all parse the same way
            // they do in the existing Update Collections modal.
            var validation = Helper.ValidateNamespaceFormat(_formNamespaces, MigrationJob.JobType);
            if (!validation.Item1)
            {
                _error = validation.Item3;
                return;
            }

            List<MigrationUnit> resolvedUnits;
            try
            {
                resolvedUnits = await Helper.PopulateJobCollectionsAsync(MigrationJob, _formNamespaces, SourceConnectionString ?? string.Empty);
            }
            catch (Exception ex)
            {
                // Surface the full exception detail (including nested inner exceptions) so the root cause
                // — e.g. an invalid source connection string / unreachable cluster — is visible to the user.
                var details = new System.Text.StringBuilder();
                for (var current = ex; current != null; current = current.InnerException)
                {
                    if (details.Length > 0) details.Append(" → ");
                    details.Append(current.Message);
                }
                _error = $"Failed to resolve namespaces: {details}";
                return;
            }

            if (resolvedUnits == null || resolvedUnits.Count == 0)
            {
                _error = "No collections matched the supplied namespaces.";
                return;
            }

            int skipped = 0;
            foreach (var mu in resolvedUnits)
            {
                // For a wildcard / CSV expansion any per-row filter typed in the form
                // applies to every resolved row unless the JSON itself carries a filter.
                var rowFilter = string.IsNullOrWhiteSpace(mu.UserFilter) ? _formFilter : mu.UserFilter;
                if (!AddDraft(mu.DatabaseName, mu.CollectionName, mu.TargetDatabaseName, mu.TargetCollectionName, rowFilter))
                {
                    skipped++;
                }
            }

            if (skipped > 0 && _error == null)
            {
                _error = $"{skipped} namespace(s) were skipped because they already exist in the job or in the draft list.";
            }

            RefreshPagination();
            _showAddPanel = false;
            ResetAddForm();
        }

        private bool AddDraft(string db, string col, string? targetDb, string? targetCol, string? filter)
        {
            var ns = $"{db}.{col}";

            if (_liveUnits.Any(u => $"{u.DatabaseName}.{u.CollectionName}" == ns))
            {
                return false;
            }
            if (_drafts.Any(d => $"{d.DatabaseName}.{d.CollectionName}" == ns))
            {
                return false;
            }

            // Sync Only skips the data copy — never overwrite/drop the target, only append.
            var formOverwrite = IsSyncOnlyJob ? false : _formOverwrite;

            var normalized = DraftOptionRules.Normalize(
                formOverwrite, _formIndexing, _formSharding, _formMoveToShard,
                MigrationJob.IsSimulatedRun);

            if (IsRuOptimizedCopyJob)
            {
                normalized = new DraftOptionRules.Normalized(formOverwrite, IndexingStrategy.DontIndex, null, null);
                filter = null;
            }

            _drafts.Add(new PendingAddition
            {
                DatabaseName = db,
                CollectionName = col,
                TargetDatabaseName = targetDb,
                TargetCollectionName = targetCol,
                Filter = filter,
                Overwrite = normalized.Overwrite,
                IndexingStrategy = normalized.Indexing,
                ShardingStrategy = normalized.Sharding,
                MoveToShard = normalized.MoveToShard,
                DataTypeForId = IsRuOptimizedCopyJob ? null : _formDataTypeForId
            });
            return true;
        }

        private void RemoveDraft(Guid id)
        {
            _drafts.RemoveAll(d => d.Id == id);
            _selectedDraftIds.Remove(id);
            RefreshPagination();
        }

        private void QueueForRemoval(string unitId)
        {
            _toRemoveIds.Add(unitId);
            StateHasChanged();
        }

        private void UndoRemoval(string unitId)
        {
            _toRemoveIds.Remove(unitId);
            StateHasChanged();
        }

        private void ToggleEditPanel(Guid draftId)
        {
            if (_expandedDraftId == draftId)
            {
                _expandedDraftId = null;
            }
            else
            {
                _expandedDraftId = draftId;
                _showAddPanel = false; // Close add panel if open
                _pendingScrollDraftId = draftId;

                // Populate form with draft values
                var draft = _drafts.FirstOrDefault(d => d.Id == draftId);
                if (draft != null)
                {
                    _formOverwrite = draft.Overwrite;
                    _formIndexing = draft.IndexingStrategy;
                    _formSharding = draft.ShardingStrategy;
                    _formMoveToShard = draft.MoveToShard;
                    _formFilter = draft.Filter;
                    _formDataTypeForId = draft.DataTypeForId;
                }
            }
        }

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender && OnReady.HasDelegate)
            {
                await OnReady.InvokeAsync();
            }

            if (_pendingScrollDraftId.HasValue)
            {
                var id = $"draft-edit-{_pendingScrollDraftId.Value}";
                _pendingScrollDraftId = null;
                try
                {
                    await JS.InvokeVoidAsync("scrollToElement", id);
                }
                catch { /* JS may be unavailable during prerender */ }
            }
        }

        private void SaveEdit(Guid draftId)
        {
            var draft = _drafts.FirstOrDefault(d => d.Id == draftId);
            if (draft == null) return;

            // Validate filter if provided. Use the MongoDB parser (relaxed/extended JSON) so the
            // validation matches what the migration pipeline actually uses at runtime — this accepts
            // query operators like $exists/$gte, unquoted field names, and extended-JSON types.
            if (!IsRuOptimizedCopyJob && !string.IsNullOrWhiteSpace(_formFilter))
            {
                if (!MongoHelper.IsValidFilter(_formFilter, out var filterError))
                {
                    _error = $"Invalid filter: {filterError}";
                    return;
                }
            }

            // Update draft with new values (create new record since it's immutable).
            // Run through the central rules helper so disabled-form fields don't leak stale values.
            // Sync Only skips the data copy — never overwrite/drop the target, only append.
            var formOverwrite = IsSyncOnlyJob ? false : _formOverwrite;
            var normalized = DraftOptionRules.Normalize(
                formOverwrite, _formIndexing, _formSharding, _formMoveToShard,
                MigrationJob.IsSimulatedRun);
            if (IsRuOptimizedCopyJob)
            {
                normalized = new DraftOptionRules.Normalized(formOverwrite, IndexingStrategy.DontIndex, null, null);
            }
            var index = _drafts.IndexOf(draft);
            _drafts[index] = draft with
            {
                Overwrite = normalized.Overwrite,
                IndexingStrategy = normalized.Indexing,
                ShardingStrategy = normalized.Sharding,
                MoveToShard = normalized.MoveToShard,
                Filter = IsRuOptimizedCopyJob ? null : _formFilter,
                DataTypeForId = IsRuOptimizedCopyJob ? null : _formDataTypeForId
            };

            _expandedDraftId = null;
            _error = null;
            RefreshPagination();
        }

        private void CancelEdit()
        {
            _expandedDraftId = null;
        }

        // === SELECTION ===

        private int GetSelectedCount()
        {
            return _selectedLiveIds.Count + _selectedDraftIds.Count;
        }

        private bool AreAllSelectedDrafts()
        {
            return _selectedLiveIds.Count == 0 && _selectedDraftIds.Count > 0;
        }

        private bool AllSelectedHaveDontShard()
        {
            return _selectedDraftIds.All(id =>
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                return draft?.ShardingStrategy == ShardingStrategy.DontShard;
            });
        }

        // Matches the form's "Locked (Overwrite = No)" rule via the central DraftOptionRules helper.
        // Bulk Set buttons for Indexing/Sharding/Move-to are disabled if any selected draft is locked.
        private bool AnySelectedHasOverwriteFalse()
        {
            return _selectedDraftIds.Any(id =>
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                return draft != null && DraftOptionRules.IsIndexingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun);
            });
        }

        private void ToggleSelectAll(ChangeEventArgs e)
        {
            var isChecked = (bool)(e.Value ?? false);
            var filtered = _paginationHelper.GetFilteredItems();
            var filteredLiveIds = filtered.OfType<MigrationUnit>()
                .Where(u => !_toRemoveIds.Contains(u.Id))
                .Select(u => u.Id)
                .ToList();
            var filteredDraftIds = filtered.OfType<PendingAddition>()
                .Select(d => d.Id)
                .ToList();

            if (isChecked)
            {
                foreach (var id in filteredLiveIds) _selectedLiveIds.Add(id);
                foreach (var id in filteredDraftIds) _selectedDraftIds.Add(id);
            }
            else
            {
                foreach (var id in filteredLiveIds) _selectedLiveIds.Remove(id);
                foreach (var id in filteredDraftIds) _selectedDraftIds.Remove(id);
            }
        }

        private bool IsAllSelected()
        {
            var filtered = _paginationHelper.GetFilteredItems();
            var liveIds = filtered.OfType<MigrationUnit>()
                .Where(u => !_toRemoveIds.Contains(u.Id))
                .Select(u => u.Id)
                .ToList();
            var draftIds = filtered.OfType<PendingAddition>()
                .Select(d => d.Id)
                .ToList();

            if (liveIds.Count + draftIds.Count == 0) return false;
            return liveIds.All(_selectedLiveIds.Contains) && draftIds.All(_selectedDraftIds.Contains);
        }

        // === BULK ACTIONS ===

        private void BulkRemove()
        {
            foreach (var id in _selectedLiveIds)
            {
                QueueForRemoval(id);
            }

            var draftsToRemove = _selectedDraftIds.ToList();
            foreach (var id in draftsToRemove)
            {
                RemoveDraft(id);
            }

            _selectedLiveIds.Clear();
            _selectedDraftIds.Clear();
        }

        private void BulkSetOverwrite(bool value)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null) continue;

                // Sync Only skips the data copy — never overwrite/drop the target, only append.
                var normalized = DraftOptionRules.Normalize(
                    IsSyncOnlyJob ? false : value, draft.IndexingStrategy, draft.ShardingStrategy, draft.MoveToShard,
                    MigrationJob.IsSimulatedRun);
                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with
                {
                    Overwrite = normalized.Overwrite,
                    IndexingStrategy = IsRuOptimizedCopyJob ? IndexingStrategy.DontIndex : normalized.Indexing,
                    ShardingStrategy = IsRuOptimizedCopyJob ? null : normalized.Sharding,
                    MoveToShard = IsRuOptimizedCopyJob ? null : normalized.MoveToShard,
                    Filter = IsRuOptimizedCopyJob ? null : draft.Filter,
                    DataTypeForId = IsRuOptimizedCopyJob ? null : draft.DataTypeForId
                };
            }
            RefreshPagination();
        }

        private void BulkSetIndexing(IndexingStrategy strategy)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsIndexingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { IndexingStrategy = strategy };
            }
            RefreshPagination();
        }

        private void BulkSetSharding(ShardingStrategy strategy)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsShardingLocked(draft.Overwrite, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                // Switching away from DontShard invalidates MoveToShard; run through Normalize.
                var normalized = DraftOptionRules.Normalize(
                    draft.Overwrite, draft.IndexingStrategy, strategy, draft.MoveToShard,
                    MigrationJob.IsSimulatedRun);
                _drafts[index] = draft with
                {
                    ShardingStrategy = normalized.Sharding,
                    MoveToShard = normalized.MoveToShard
                };
            }
            RefreshPagination();
        }

        private void BulkSetMoveToAuto()
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsMoveToShardLocked(draft.Overwrite, draft.ShardingStrategy, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { MoveToShard = null };
            }
            RefreshPagination();
        }

        private void BulkSetMoveToShard(string shardId)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null || DraftOptionRules.IsMoveToShardLocked(draft.Overwrite, draft.ShardingStrategy, MigrationJob.IsSimulatedRun)) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { MoveToShard = shardId };
            }
            RefreshPagination();
        }

        private void BulkClearFilter()
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { Filter = null };
            }
            RefreshPagination();
        }

        // null clears the user pin (=> "Unknown / Multiple"); a value pins the _id type for selected drafts.
        private void BulkSetDataTypeForId(DataType? dataType)
        {
            foreach (var id in _selectedDraftIds)
            {
                var draft = _drafts.FirstOrDefault(d => d.Id == id);
                if (draft == null) continue;

                var index = _drafts.IndexOf(draft);
                _drafts[index] = draft with { DataTypeForId = dataType };
            }
            RefreshPagination();
        }

        // === CLUSTER NODES ===

        private async Task EnsureClusterNodesLoaded()
        {
            if (_clusterNodes.Count > 0 || _loadingNodes) return;

            _loadingNodes = true;
            try
            {
                _clusterNodes = await MongoHelper.GetClusterNodesAsync(TargetConnectionString);
            }
            catch (Exception ex)
            {
                MigrationJobContext.AddVerboseLog($"Failed to load cluster nodes: {ex.Message}");
            }
            finally
            {
                _loadingNodes = false;
            }
        }

        // === NAVIGATION ===

        private int GetPendingAddCount() => _drafts.Count;
        private int GetPendingRemoveCount() => _toRemoveIds.Count;

        private async Task ShowSummary()
        {
            _isPreparingSummary = true;
            // Paint the busy overlay before building the (potentially very large) summary table.
            StateHasChanged();
            await Task.Yield();

            try
            {
                _showSummary = true;
            }
            finally
            {
                _isPreparingSummary = false;
            }
        }

        private void BackToList()
        {
            _showSummary = false;
        }

        private async Task Cancel()
        {
            await OnCancelled.InvokeAsync();
        }

        // === SUMMARY VIEW ===

        private bool HasConflicts()
        {
            return _drafts.Any(d =>
            {
                var ns = $"{d.DatabaseName}.{d.CollectionName}";
                return _toRemoveIds.Any(id =>
                {
                    var u = _liveUnits.FirstOrDefault(x => x.Id == id);
                    return u != null && $"{u.DatabaseName}.{u.CollectionName}" == ns;
                });
            });
        }

        private string TruncateFilter(string filter)
        {
            const int maxLen = 50;
            return filter.Length > maxLen ? filter.Substring(0, maxLen) + "..." : filter;
        }

        private async Task ApplyChanges()
        {
            _isApplying = true;
            _error = null;

            // Force a re-render so the busy overlay paints before the (potentially long-running,
            // mostly-synchronous) submit work starts. Without this, with thousands of drafts the
            // user sees a frozen UI for several seconds before anything updates.
            StateHasChanged();
            await Task.Yield();

            try
            {
                // Step 1: Remove collections. Sync-back guard: clear change-stream resume tokens
                // before removal so a later re-add of the same namespace starts a fresh stream.
                foreach (var id in _toRemoveIds)
                {
                    var unit = _liveUnits.FirstOrDefault(u => u.Id == id);
                    if (unit != null)
                    {
                        unit.ResumeToken = null;
                        unit.OriginalResumeToken = null;
                        unit.CSLastResumeTokenWithChange = null;
                        unit.CSLastChangeUTCTime = null;
                        unit.SyncBackResumeToken = null;
                        unit.SyncBackOriginalResumeToken = null;
                        unit.SyncBackCSLastResumeTokenWithChange = null;
                        unit.SyncBackCSLastChangeUTCTime = null;

                        var jobIndex = MigrationJob.MigrationUnitBasics?.FindIndex(mu => mu.Id == id) ?? -1;
                        if (jobIndex >= 0)
                        {
                            MigrationJob.MigrationUnitBasics!.RemoveAt(jobIndex);
                        }

                        // Centralized purge: deletes the persisted MU file, evicts the cache entry,
                        // drops the per-MU mutate lock, and clears any in-memory state held by the
                        // active change-stream processor so a re-add of the same namespace (which
                        // regenerates the same MU id) starts from a clean slate.
                        MigrationJobContext.PurgeMigrationUnit(MigrationJob.Id, id);

                        _liveUnits.RemoveAll(u => u.Id == id);
                    }
                }

                // Step 2: Add drafts as new MigrationUnits
                foreach (var draft in _drafts)
                {
                    var effectiveDraft = draft;
                    if (IsRuOptimizedCopyJob)
                    {
                        effectiveDraft = draft with
                        {
                            IndexingStrategy = IndexingStrategy.DontIndex,
                            ShardingStrategy = null,
                            MoveToShard = null,
                            Filter = null,
                            DataTypeForId = null
                        };
                    }

                    var unit = new MigrationUnit(MigrationJob, draft.DatabaseName, draft.CollectionName, new List<MigrationChunk>());
                    effectiveDraft.ApplyToMigrationUnit(unit);

                    MigrationJob.MigrationUnitBasics ??= new List<MigrationUnitBasic>();
                    var addedBasic = unit.GetBasic();
                    MigrationJob.MigrationUnitBasics.Add(addedBasic);

                    unit.Persist();
                }

                // Step 3: Save job
                if (!MigrationJobContext.SaveMigrationJob(MigrationJob))
                {
                    _error = "Failed to save migration job.";
                    return;
                }

                // Step 4: Fire callback and close
                await OnCollectionsCommitted.InvokeAsync();
            }
            catch (Exception ex)
            {
                _error = $"Error applying changes: {ex.Message}";
            }
            finally
            {
                _isApplying = false;
            }
        }

        // === UPLOAD FILE ===

        private async Task OnUploadNamespacesFile(InputFileChangeEventArgs e)
        {
            _error = null;
            var file = e.File;
            if (file == null) return;

            // Cap at 1 MB to protect against accidental huge uploads.
            const long maxBytes = 1024 * 1024;
            if (file.Size > maxBytes)
            {
                _error = $"Upload rejected: file is {file.Size} bytes, max {maxBytes}.";
                return;
            }

            try
            {
                using var stream = file.OpenReadStream(maxBytes);
                using var reader = new System.IO.StreamReader(stream);
                var contents = await reader.ReadToEndAsync();
                _formNamespaces = string.IsNullOrWhiteSpace(_formNamespaces)
                    ? contents
                    : _formNamespaces.TrimEnd() + Environment.NewLine + contents;
            }
            catch (Exception ex)
            {
                _error = $"Failed to read uploaded file: {ex.Message}";
            }
        }

        // === FILTER ===

        private void OnFilterChanged()
        {
            RefreshPagination();
        }

        private void ClearFilter()
        {
            _paginationHelper.FilterText = string.Empty;
            RefreshPagination();
        }
    }
}

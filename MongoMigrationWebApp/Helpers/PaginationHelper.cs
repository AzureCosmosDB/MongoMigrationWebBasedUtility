using System;
using System.Collections.Generic;
using System.Linq;

namespace MongoMigrationWebApp.Helpers
{
    public class PaginationHelper<T>
    {
        private List<T> _allItems;
        private readonly Func<T, string, bool> _filterPredicate;
        private string _filterText = string.Empty;
        private int _currentPage = 1;
        private int _pageSize = 10;

        public PaginationHelper(List<T> items, Func<T, string, bool> filterPredicate)
        {
            _allItems = items ?? new List<T>();
            _filterPredicate = filterPredicate;
        }

        public PaginationHelper(List<T> items, int pageSize, Func<T, string, bool> filterPredicate)
            : this(items, filterPredicate)
        {
            _pageSize = Math.Max(1, pageSize);
        }

        public void UpdateSource(List<T> items)
        {
            _allItems = items ?? new List<T>();
            if (_currentPage > GetTotalPages()) _currentPage = GetTotalPages();
            if (_currentPage < 1) _currentPage = 1;
        }

        public int TotalPages => GetTotalPages();

        public List<T> GetCurrentPageItems() => GetPagedItems();

        public string FilterText
        {
            get => _filterText;
            set
            {
                _filterText = value;
                _currentPage = 1; // Reset to first page when filter changes
            }
        }

        public int CurrentPage
        {
            get => _currentPage;
            set => _currentPage = Math.Max(1, Math.Min(value, GetTotalPages()));
        }

        public int PageSize
        {
            get => _pageSize;
            set
            {
                _pageSize = Math.Max(1, value);
                _currentPage = 1; // Reset to first page when page size changes
            }
        }

        public int TotalItemsCount => _allItems.Count;

        public List<T> GetFilteredItems()
        {
            if (string.IsNullOrWhiteSpace(_filterText))
                return _allItems;

            return _allItems.Where(item => _filterPredicate(item, _filterText)).ToList();
        }

        public List<T> GetPagedItems()
        {
            var filteredItems = GetFilteredItems();
            var skip = (_currentPage - 1) * _pageSize;
            return filteredItems.Skip(skip).Take(_pageSize).ToList();
        }

        public int GetFilteredItemsCount()
        {
            return GetFilteredItems().Count;
        }

        public string GetDisplayedItemsInfo()
        {
            var filteredCount = GetFilteredItemsCount();
            if (filteredCount == 0)
                return "0";

            var start = (_currentPage - 1) * _pageSize + 1;
            var end = Math.Min(_currentPage * _pageSize, filteredCount);
            return $"{start}-{end}";
        }

        public int GetTotalPages()
        {
            var filteredCount = GetFilteredItemsCount();
            return filteredCount == 0 ? 1 : (int)Math.Ceiling((double)filteredCount / _pageSize);
        }

        public bool IsFirstPage() => _currentPage == 1;

        public bool IsLastPage() => _currentPage >= GetTotalPages();

        public int GetStartPage()
        {
            var totalPages = GetTotalPages();
            var startPage = Math.Max(1, _currentPage - 2);
            return Math.Min(startPage, Math.Max(1, totalPages - 4));
        }

        public int GetEndPage()
        {
            var totalPages = GetTotalPages();
            var endPage = Math.Min(totalPages, _currentPage + 2);
            return Math.Max(endPage, Math.Min(5, totalPages));
        }

        public void GoToPage(int pageNumber)
        {
            if (pageNumber >= 1 && pageNumber <= GetTotalPages())
            {
                _currentPage = pageNumber;
            }
        }

        public void ClearFilter()
        {
            _filterText = string.Empty;
            _currentPage = 1;
        }

        public bool ShowPagination()
        {
            return GetTotalPages() > 1;
        }

        public bool ShowFilter()
        {
            return _allItems.Count > 25;
        }
    }
}
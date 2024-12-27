package utils

import "sort"

func partition[T any](arr []T, low, high int, cmp func(a, b T) bool) int {
	pivot := arr[high]
	i := low
	for j := low; j < high; j++ {
		if cmp(arr[j], pivot) {
			arr[i], arr[j] = arr[j], arr[i]
			i++
		}
	}
	arr[i], arr[high] = arr[high], arr[i]
	return i
}

func quickSelect[T any](arr []T, low, high, k int, cmp func(a, b T) bool) {
	if low < high {
		p := partition(arr, low, high, cmp)
		if p == k {
			return
		} else if p > k {
			quickSelect[T](arr, low, p-1, k, cmp)
		} else {
			quickSelect[T](arr, p+1, high, k, cmp)
		}
	}
}

func TopN[T any](items []T, n int, cmp func(a, b T) bool) []T {
	if len(items) <= n {
		result := make([]T, len(items))
		copy(result, items)
		sort.Slice(result, func(i, j int) bool {
			return cmp(result[i], result[j])
		})
		return result
	}

	quickSelect[T](items, 0, len(items)-1, n-1, cmp)

	topItems := items[:n]
	sort.Slice(topItems, func(i, j int) bool {
		return cmp(topItems[i], topItems[j])
	})

	return topItems
}

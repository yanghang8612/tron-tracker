package utils

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
		for i := 0; i < len(result); i++ {
			for j := i + 1; j < len(result); j++ {
				if !cmp(result[i], result[j]) {
					result[i], result[j] = result[j], result[i]
				}
			}
		}
		return result
	}

	quickSelect[T](items, 0, len(items)-1, n-1, cmp)

	topItems := items[:n]
	for i := 0; i < len(topItems); i++ {
		for j := i + 1; j < len(topItems); j++ {
			if !cmp(topItems[i], topItems[j]) {
				topItems[i], topItems[j] = topItems[j], topItems[i]
			}
		}
	}

	return topItems
}

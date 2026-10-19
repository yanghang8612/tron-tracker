package google

import (
	"fmt"
	"strings"

	"tron-tracker/common"
	"tron-tracker/database/models"
	"tron-tracker/net"

	"google.golang.org/api/slides/v1"
)

type exchangeStakeTotals struct {
	bandwidth int64 // sun
	energy    int64 // sun
}

// Sum all exchanges, including HTX. The Top10/HTX chart filters do not apply
// to the summary cards, whose labels describe CEX totals.
func sumExchangeStake(stats map[string]*models.ExchangeResourceStatistic) (exchangeStakeTotals, error) {
	if len(stats) == 0 {
		return exchangeStakeTotals{}, fmt.Errorf("exchange resource snapshot is missing")
	}
	var total exchangeStakeTotals
	for _, stat := range stats {
		if stat == nil {
			return exchangeStakeTotals{}, fmt.Errorf("exchange resource snapshot contains a nil row")
		}
		if stat.Bandwidth < 0 || stat.Energy < 0 {
			return exchangeStakeTotals{}, fmt.Errorf("exchange resource snapshot contains negative stake for %s", stat.Name)
		}
		total.bandwidth += stat.Bandwidth
		total.energy += stat.Energy
	}
	return total, nil
}

func exchangeStakeChange(previous, current int64, hasPrevious bool) string {
	if !hasPrevious || (previous == 0 && current != 0) {
		return "N/A"
	}
	if previous == 0 {
		return "+0.00%"
	}
	return common.FormatChangePercent(previous, current)
}

// Find a card's value by its label within the same group, rather than fragile
// PageElements offsets or document-specific IDs. Fail before any writes if
// the template has become ambiguous or a required value shape is missing.
func findExchangeStakeSummaryElements(page *slides.Page) (map[string]*slides.PageElement, error) {
	labels := []string{"cex total bandwidth", "of total net bandwidth", "cex total energy", "of total net energy", "cex total stake", "of total network"}
	found := make(map[string]*slides.PageElement, len(labels))
	var walk func([]*slides.PageElement) error
	walk = func(elements []*slides.PageElement) error {
		for _, element := range elements {
			if element.ElementGroup == nil {
				continue
			}
			children := element.ElementGroup.Children
			for _, label := range labels {
				for _, child := range children {
					if strings.ToLower(strings.Join(strings.Fields(extractText(child)), " ")) != label {
						continue
					}
					var value *slides.PageElement
					for _, sibling := range children {
						if sibling != child && sibling.Shape != nil && strings.TrimSpace(extractText(sibling)) != "" {
							if value != nil {
								return fmt.Errorf("multiple value shapes for %q", label)
							}
							value = sibling
						}
					}
					if value == nil || found[label] != nil {
						return fmt.Errorf("missing or duplicate value shape for %q", label)
					}
					found[label] = value
				}
			}
			if err := walk(children); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(page.PageElements); err != nil {
		return nil, err
	}
	for _, label := range labels {
		if found[label] == nil {
			return nil, fmt.Errorf("missing exchange stake card %q", label)
		}
	}
	return found, nil
}

// Replace only the content before the final paragraph newline, then rebuild
// the two original style runs at their new boundaries. Preserve font family,
// font sizes, emphasis, paragraph formatting, and group geometry.
func buildExchangeStakeCardRequests(element *slides.PageElement, primary, secondary string, isChange bool) ([]*slides.Request, error) {
	var styles []*slides.TextStyle
	for _, part := range element.Shape.Text.TextElements {
		if part.TextRun != nil && strings.TrimSpace(part.TextRun.Content) != "" {
			styles = append(styles, part.TextRun.Style)
		}
	}
	if len(styles) < 2 || styles[0] == nil || styles[len(styles)-1] == nil {
		return nil, fmt.Errorf("card %s must have primary and secondary text styles", element.ObjectId)
	}
	text := primary + " " + secondary
	oldText := strings.TrimSuffix(extractText(element), "\n")
	reqs := buildUpdateTextRequests(element.ObjectId, -1, -1, 0, utf16Len(oldText), text)
	start, split, end := int64(0), utf16Len(primary+" "), utf16Len(text)
	primaryStyle, secondaryStyle := *styles[0], *styles[len(styles)-1]
	if isChange {
		color := &slides.RgbColor{Red: 1, Green: 1, Blue: 1}
		if strings.HasPrefix(secondary, "-") {
			color = &slides.RgbColor{Red: 1}
		} else if strings.HasPrefix(secondary, "+") {
			color = &slides.RgbColor{Green: 1}
		}
		secondaryStyle.ForegroundColor = &slides.OptionalColor{OpaqueColor: &slides.OpaqueColor{RgbColor: color}}
	}
	for _, run := range []struct {
		start, end int64
		style      *slides.TextStyle
	}{{start, split, &primaryStyle}, {split, end, &secondaryStyle}} {
		reqs = append(reqs, &slides.Request{UpdateTextStyle: &slides.UpdateTextStyleRequest{
			ObjectId:  element.ObjectId,
			TextRange: &slides.Range{Type: "FIXED_RANGE", StartIndex: &run.start, EndIndex: &run.end},
			Style:     run.style,
			Fields:    "fontSize,fontFamily,weightedFontFamily,bold,italic,foregroundColor",
		}})
	}
	return reqs, nil
}

func buildExchangeStakeSummaryRequests(page *slides.Page, current, previous exchangeStakeTotals, hasPrevious bool, network net.NetworkStakeTotals) ([]*slides.Request, error) {
	if network.BandwidthTRX <= 0 || network.EnergyTRX <= 0 {
		return nil, fmt.Errorf("network staking totals must be positive")
	}
	elements, err := findExchangeStakeSummaryElements(page)
	if err != nil {
		return nil, err
	}
	var reqs []*slides.Request
	for _, card := range []struct {
		label, shareLabel string
		current, previous int64
		networkTRX        int64
	}{
		{"cex total bandwidth", "of total net bandwidth", current.bandwidth, previous.bandwidth, network.BandwidthTRX},
		{"cex total energy", "of total net energy", current.energy, previous.energy, network.EnergyTRX},
		{"cex total stake", "of total network", current.bandwidth + current.energy, previous.bandwidth + previous.energy, network.BandwidthTRX + network.EnergyTRX},
	} {
		currentTRX := float64(card.current) / 1e6
		totalReqs, err := buildExchangeStakeCardRequests(elements[card.label], common.FormatWithUnits(currentTRX)+" TRX", exchangeStakeChange(card.previous, card.current, hasPrevious), true)
		if err != nil {
			return nil, err
		}
		shareReqs, err := buildExchangeStakeCardRequests(elements[card.shareLabel], fmt.Sprintf("%.2f%%", currentTRX/float64(card.networkTRX)*100), "(total "+common.FormatWithUnits(float64(card.networkTRX))+")", false)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, totalReqs...)
		reqs = append(reqs, shareReqs...)
	}
	return reqs, nil
}

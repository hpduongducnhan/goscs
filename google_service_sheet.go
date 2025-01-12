package goscs

import "google.golang.org/api/sheets/v4"

func ReadSheetToRange(sheetService *sheets.Service, spreadsheetId string, readRange string) (*sheets.ValueRange, error) {
	resp, err := sheetService.Spreadsheets.Values.Get(spreadsheetId, readRange).Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func WriteSheetToRange(sheetService *sheets.Service, spreadsheetId string, writeRange string, data [][]interface{}) (*sheets.UpdateValuesResponse, error) {
	// Write the data to the spreadsheet
	valueRange := &sheets.ValueRange{
		Values: data,
	}

	resp, err := sheetService.Spreadsheets.Values.Update(spreadsheetId, writeRange, valueRange).ValueInputOption("RAW").Do()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

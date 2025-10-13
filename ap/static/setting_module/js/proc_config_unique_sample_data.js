const changeSampleDataDisplayMode = async (e) => {
    const sampleDataDisplayMode = e.value;
    const spreadsheet = getSpreadSheetFromToolsBarElement(e);
    await SpreadSheetProcessConfig.updateSampleDataByDisplayMode(spreadsheet, sampleDataDisplayMode);
};

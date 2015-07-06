package hms.wikidata.model;

public class WikiReference {

	
	private String sourceItemId ;
	private String claimId; 
	private String referenceId;
	private String referenceValue;
	private String referenceValueType ;
	
	
	public String getSourceItemId() {
		return sourceItemId;
	}
	public void setSourceItemId(String sourceItemId) {
		this.sourceItemId = sourceItemId;
	}
	public String getClaimId() {
		return claimId;
	}
	public void setClaimId(String claimId) {
		this.claimId = claimId;
	}
	public String getReferenceId() {
		return referenceId;
	}
	public void setReferenceId(String referenceId) {
		this.referenceId = referenceId;
	}
	public String getReferenceValue() {
		return referenceValue;
	}
	public void setReferenceValue(String referenceValue) {
		this.referenceValue = referenceValue;
	}
	public String getReferenceValueType() {
		return referenceValueType;
	}
	public void setReferenceValueType(String referenceValueType) {
		this.referenceValueType = referenceValueType;
	}
	
	
	
}

package hms.wikidata.model;

public class ClaimItem
{

    private String propertyID;
    private String propertyName;
    private String propertyDataType;
    private String propertyValue;
    public String getPropertyID()
    {
        return propertyID;
    }
    public void setPropertyID(String propertyID)
    {
        this.propertyID = propertyID;
    }
    public String getPropertyName()
    {
        return propertyName;
    }
    public void setPropertyName(String propertyName)
    {
        this.propertyName = propertyName;
    }
    public String getPropertyDataType()
    {
        return propertyDataType;
    }
    public void setPropertyDataType(String propertyDataType)
    {
        this.propertyDataType = propertyDataType;
    }
    public String getPropertyValue()
    {
        return propertyValue;
    }
    public void setPropertyValue(String propertyValue)
    {
        this.propertyValue = propertyValue;
    }

    @Override
    public String toString()
    {
        return "ID: " + this.propertyID + "\t Name: " +propertyName + "\t Type: " + propertyDataType + "\t Value:" + propertyValue  ;
    }

}

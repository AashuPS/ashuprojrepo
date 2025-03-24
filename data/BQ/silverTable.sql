MERGE INTO `ashuproj-454704.silver_dataset.customers` target
USING 
  (SELECT DISTINCT
    customer_id, name, email, updated_at, 
    CASE 
      WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE
      ELSE FALSE
    END AS is_quarantined,
    CURRENT_TIMESTAMP() AS effective_start_date,
    NULL AS effective_end_date,
    TRUE AS is_active
  FROM `ashuproj-454704.bronze_dataset.customers`) source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN MATCHED AND 
            (
             target.name IS DISTINCT FROM source.name OR
             target.email IS DISTINCT FROM source.email OR
             target.updated_at IS DISTINCT FROM source.updated_at) 
    THEN UPDATE SET 
        target.is_active = FALSE,
        target.effective_end_date = CURRENT_TIMESTAMP();

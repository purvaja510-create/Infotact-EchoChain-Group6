\# EchoChain Architecture



\## High-Level Architecture



EchoChain follows an end-to-end data engineering and analytics pipeline.



```text

Secondary Market Data

&#x20;       |

&#x20;       v

&#x20;    Scrapy

&#x20;       |

&#x20;       v

&#x20;  Raw JSON / CSV

&#x20;       |

&#x20;       v

&#x20;Databricks / Delta Lake

&#x20;       |

&#x20;       v

&#x20;     Bronze

&#x20;       |

&#x20;       v

&#x20;    PySpark

&#x20;       |

&#x20;       v

&#x20;     Silver

&#x20;       |

&#x20;       v

&#x20;Fuzzy SKU Matching

&#x20;       |

&#x20;       v

&#x20;      Gold

&#x20;       |

&#x20;       v

&#x20;    Power BI

&#x20;       |

&#x20;       v

Executive Analytics

